package core

import (
	"context"
	"errors"
	"log"
	"sync"

	"codeberg.org/eljoth/tram/internal/core/domain"
	"codeberg.org/eljoth/tram/internal/ports"
)

type Core struct {
	ctx           context.Context
	mutex         sync.RWMutex
	wg            sync.WaitGroup
	closed        bool
	streams       map[string]*stream
	sourceFactory ports.Factory[domain.SourceSpec, domain.Source]
	stageFactory  ports.Factory[domain.StageSpec, domain.Stage]
	sinkFactory   ports.Factory[domain.SinkSpec, domain.Sink]
	watchers      map[*watcher]struct{}
}

// stream is a built tree whose source is running.
type stream struct {
	tree *domain.Tree
	// done is closed once the source's Run has returned.
	done chan struct{}
}

type watcher struct {
	ctx context.Context
	ch  chan<- domain.StreamSpecWatchEvent
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
		streams:       map[string]*stream{},
		sourceFactory: sourceFactory,
		stageFactory:  stageFactory,
		sinkFactory:   sinkFactory,
		watchers:      map[*watcher]struct{}{},
	}
}

func (c *Core) UpsertStream(ctx context.Context, spec domain.StreamSpec) (*domain.StreamSpec, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.closed {
		return nil, domain.ErrCoreClosed
	}

	// Reject structurally invalid specs before touching the running stream.
	tree, err := newTree(spec)
	if err != nil {
		return nil, errors.Join(domain.ErrStreamBuildFailed, err)
	}

	// TODO instead of stopping and starting a stream, modify its nodes while running.

	// The old stream is stopped before the new one is built, because its
	// components may hold resources (e.g. listening ports) the new ones need.
	old, exists := c.streams[spec.Name]
	if exists {
		if err := c.stopStream(old); err != nil {
			log.Printf("failed to cleanly stop stream %s: %v", spec.Name, err)
		}
		delete(c.streams, spec.Name)
	}

	if err := c.buildStream(tree); err != nil {
		if exists {
			c.restoreStream(old.tree.Spec)
		}
		return nil, errors.Join(domain.ErrStreamBuildFailed, err)
	}

	c.runStream(tree)
	c.notify(domain.StreamSpecWatchEventTypeUpsert, tree.Spec)

	return &tree.Spec, nil
}

func (c *Core) DeleteStream(ctx context.Context, name string) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.closed {
		return domain.ErrCoreClosed
	}

	stream, exists := c.streams[name]
	if !exists {
		return domain.ErrStreamNotFound
	}

	log.Println("Stopping stream", name)
	// Every component has been asked to stop even if some failed, so the
	// stream is removed either way.
	if err := c.stopStream(stream); err != nil {
		log.Printf("failed to cleanly stop stream %s: %v", name, err)
	}

	delete(c.streams, name)
	c.notify(domain.StreamSpecWatchEventTypeDelete, stream.tree.Spec)
	log.Println("Deleted stream", name)

	return nil
}

func (c *Core) GetStream(ctx context.Context, name string) (*domain.StreamSpec, error) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	stream, exists := c.streams[name]
	if !exists {
		return nil, domain.ErrStreamNotFound
	}
	return &stream.tree.Spec, nil
}

func (c *Core) GetStreams(ctx context.Context) ([]domain.StreamSpec, error) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	streams := make([]domain.StreamSpec, 0, len(c.streams))
	for _, stream := range c.streams {
		streams = append(streams, stream.tree.Spec)
	}
	return streams, nil
}

func (c *Core) Close() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.closed = true

	for name, stream := range c.streams {
		log.Printf("stopping stream %s", name)
		if err := c.stopStream(stream); err != nil {
			log.Printf("failed to stop stream %s: %v", name, err)
		}
	}
	clear(c.streams)

	c.wg.Wait()

	return nil
}

// Watch registers ch to receive every stream change until ctx is done.
// Events are delivered synchronously, so the caller has to keep receiving
// from ch until it cancels ctx.
func (c *Core) Watch(ctx context.Context, ch chan domain.StreamSpecWatchEvent) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.closed {
		return domain.ErrCoreClosed
	}

	w := &watcher{ctx: ctx, ch: ch}
	c.watchers[w] = struct{}{}
	context.AfterFunc(ctx, func() {
		c.mutex.Lock()
		defer c.mutex.Unlock()
		delete(c.watchers, w)
	})

	return nil
}

// notify must be called with the write lock held, which keeps events in the
// same order as the changes they describe.
func (c *Core) notify(eventType domain.StreamSpecWatchEventType, spec domain.StreamSpec) {
	event := domain.StreamSpecWatchEvent{
		EventType:  eventType,
		StreamSpec: spec,
	}
	for w := range c.watchers {
		select {
		case w.ch <- event:
		case <-w.ctx.Done():
		}
	}
}
