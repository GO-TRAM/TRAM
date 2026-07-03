package core

import (
	"context"
	"sync"

	"codeberg.org/eljoth/stream/internal/core/domain"
	"codeberg.org/eljoth/stream/internal/ports"
)

type Core struct {
	ctx           context.Context
	mutex         sync.RWMutex
	streams       map[string]*domain.Stream
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
		streams:       map[string]*domain.Stream{},
		sourceFactory: sourceFactory,
		stageFactory:  stageFactory,
		sinkFactory:   sinkFactory,
	}
}

func (c *Core) UpsertStream(ctx context.Context, spec domain.StreamSpec) (*domain.StreamSpec, error) {
	stream, exists := c.getStream(spec.Name)

	c.mutex.Lock()
	defer c.mutex.Unlock()

	builtStream, err := c.buildStream(spec)
	if err != nil {
		return nil, err
	}

	if err := c.wireStream(builtStream); err != nil {
		return nil, err
	}

	if exists {
		if err := stream.Source.Stop(); err != nil {
			return nil, domain.ErrStreamStopFailed
		}
	}

	go builtStream.Source.Run(c.ctx)

	c.streams[spec.Name] = builtStream
	return &builtStream.Spec, nil
}

func (c *Core) DeleteStream(ctx context.Context, name string) error {
	stream, exists := c.getStream(name)
	if !exists {
		return domain.ErrStreamNotFound
	}

	stream.Source.Stop()
	delete(c.streams, name)

	return nil
}

func (c *Core) GetStream(ctx context.Context, name string) (*domain.StreamSpec, error) {
	stream, exists := c.getStream(name)
	if !exists {
		return &stream.Spec, nil
	}
	return nil, domain.ErrStreamNotFound
}
