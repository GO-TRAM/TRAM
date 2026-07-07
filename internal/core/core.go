package core

import (
	"context"
	"errors"
	"log"
	"sync"

	"codeberg.org/eljoth/stream/internal/core/domain"
	"codeberg.org/eljoth/stream/internal/ports"
)

type Core struct {
	ctx           context.Context
	mutex         sync.RWMutex
	wg            sync.WaitGroup
	closed        bool
	streams       map[string]*domain.Tree
	sourceFactory ports.Factory[domain.SourceSpec, domain.Source]
	stageFactory  ports.Factory[domain.StageSpec, domain.Stage]
	sinkFactory   ports.Factory[domain.SinkSpec, domain.Sink]
}

func New(
	ctx context.Context,
	sourceFactory ports.Factory[domain.SourceSpec, domain.Source],
	stageFactory ports.Factory[domain.StageSpec, domain.Stage],
	sinkFactory ports.Factory[domain.SinkSpec, domain.Sink],
) *Core {
	return &Core{
		ctx:           ctx,
		mutex:         sync.RWMutex{},
		streams:       map[string]*domain.Tree{},
		sourceFactory: sourceFactory,
		stageFactory:  stageFactory,
		sinkFactory:   sinkFactory,
	}
}

func (c *Core) UpsertStream(ctx context.Context, spec domain.StreamSpec) (*domain.StreamSpec, error) {
	if c.closed {
		return nil, domain.ErrCoreClosed
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	stream, exists := c.getStream(spec.Name)

	builtStream, err := c.buildStream(spec)
	if err != nil {
		return nil, errors.Join(domain.ErrStreamBuildFailed, err)
	}

	if exists {
		source, err := stream.Root().GetSource()
		if err != nil {
			return nil, errors.Join(domain.ErrStreamStopFailed, err)
		}
		if err := source.Stop(); err != nil {
			return nil, domain.ErrStreamStopFailed
		}
	}

	c.wg.Go(func() {
		log.Println("Running stream", builtStream.Spec.Name)
		source, err := builtStream.Root().GetSource()
		if err != nil {
			log.Println("Failed to get source for stream", builtStream.Spec.Name, err)
			return
		}
		source.Run(c.ctx)
	})

	c.streams[spec.Name] = builtStream
	return &builtStream.Spec, nil
}

func (c *Core) DeleteStream(ctx context.Context, name string) error {
	if c.closed {
		return domain.ErrCoreClosed
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	stream, exists := c.getStream(name)
	if !exists {
		return domain.ErrStreamNotFound
	}

	log.Println("Stopping stream", name)
	source, err := stream.Root().GetSource()
	if err != nil {
		return errors.Join(domain.ErrStreamStopFailed, err)
	}
	if err := source.Stop(); err != nil {
		return domain.ErrStreamStopFailed
	}

	delete(c.streams, name)
	log.Println("Deleted stream", name)

	return nil
}

func (c *Core) GetStream(ctx context.Context, name string) (*domain.StreamSpec, error) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	stream, exists := c.getStream(name)
	if !exists {
		return nil, domain.ErrStreamNotFound
	}
	return &stream.Spec, nil
}

func (c *Core) Close() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.closed = true

	for _, stream := range c.streams {
		log.Printf("stopping stream %s", stream.Spec.Name)
		source, err := stream.Root().GetSource()
		if err != nil {
			log.Printf("failed to get source for stream %s: %v", stream.Spec.Name, err)
			continue
		}
		if err := source.Stop(); err != nil {
			log.Printf("failed to stop stream %s: %v", stream.Spec.Name, err)
		}
	}

	c.wg.Wait()

	return nil
}
