package core

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"go.opentelemetry.io/otel"

	"codeberg.org/eljoth/tram/internal/core/domain"
	"codeberg.org/eljoth/tram/internal/ports"
)

// A stream whose source returned on its own is restarted after a delay that
// doubles with every restart in a row, between these bounds. A source that
// ran for at least maxRestartDelay counts as recovered.
const (
	minRestartDelay = time.Second
	maxRestartDelay = time.Minute
)

type Core struct {
	ctx           context.Context
	logger        *slog.Logger
	mutex         sync.RWMutex
	wg            sync.WaitGroup
	closed        bool
	streams       map[string]*stream
	sourceFactory ports.Factory[domain.SourceSpec, domain.Source]
	stageFactory  ports.Factory[domain.StageSpec, domain.Stage]
	sinkFactory   ports.Factory[domain.SinkSpec, domain.Sink]
	watchers      map[*watcher]struct{}
	metrics       *metrics

	minRestartDelay time.Duration
	maxRestartDelay time.Duration
}

// stream is a built tree whose source was started. The stream stays
// registered after its source returned on its own, until it is restarted.
type stream struct {
	tree *domain.Tree
	// done is closed once the source's Run has returned.
	done chan struct{}
	// stopped is set once stopStream ran, so the components, which may not
	// support being stopped twice, are stopped only once.
	stopped bool
}

// running reports whether the source's Run has not returned yet.
func (s *stream) running() bool {
	select {
	case <-s.done:
		return false
	default:
		return true
	}
}

type watcher struct {
	ctx context.Context
	ch  chan<- domain.StreamSpecWatchEvent
}

func New(
	ctx context.Context,
	logger *slog.Logger,
	sourceFactory ports.Factory[domain.SourceSpec, domain.Source],
	stageFactory ports.Factory[domain.StageSpec, domain.Stage],
	sinkFactory ports.Factory[domain.SinkSpec, domain.Sink],
) *Core {
	// The metrics go to the global meter provider, which records nothing
	// unless the application sets one up.
	m, err := newMetrics(otel.GetMeterProvider())
	if err != nil {
		logger.ErrorContext(ctx, "Could not create metrics", slog.Any("error", err))
	}

	return &Core{
		ctx:           ctx,
		logger:        logger,
		mutex:         sync.RWMutex{},
		streams:       map[string]*stream{},
		sourceFactory: sourceFactory,
		stageFactory:  stageFactory,
		sinkFactory:   sinkFactory,
		watchers:      map[*watcher]struct{}{},
		metrics:       m,

		minRestartDelay: minRestartDelay,
		maxRestartDelay: maxRestartDelay,
	}
}

func (c *Core) UpsertStream(ctx context.Context, spec domain.StreamSpec) (*domain.StreamSpec, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.closed {
		return nil, domain.ErrCoreClosed
	}

	if err := domain.ValidateStreamName(spec.Name); err != nil {
		return nil, errors.Join(domain.ErrStreamBuildFailed, err)
	}

	// Reject structurally invalid specs before touching the running stream.
	tree, err := newTree(spec)
	if err != nil {
		return nil, errors.Join(domain.ErrStreamBuildFailed, err)
	}

	old, exists := c.streams[spec.Name]
	// A stream whose source has returned is rebuilt even if its spec is
	// unchanged, so applying the spec again restarts it right away.
	running := exists && old.running()

	// Updating restarts components, so an unchanged spec is left alone.
	if running && old.tree.Spec.Equal(spec) {
		return &old.tree.Spec, nil
	}

	// With an unchanged source, only the stages and sinks are replaced while
	// the source keeps running.
	if running && old.tree.Spec.Source.Equal(spec.Source) {
		if err = c.replaceReceivers(old, tree); err != nil {
			return nil, errors.Join(domain.ErrStreamBuildFailed, err)
		}
		c.logger.InfoContext(ctx, "Replaced stages and sinks of stream", slog.String("stream", spec.Name))
		c.notify(domain.StreamSpecWatchEventTypeUpsert, tree.Spec)
		return &tree.Spec, nil
	}

	// Otherwise the old stream is stopped before the new one is built, because
	// its components may hold resources (e.g. listening ports) the new ones
	// need. It stays registered until the new one replaces it, so a failed
	// update never makes the stream disappear.
	if exists {
		if err = c.stopStream(old); err != nil {
			c.logger.ErrorContext(ctx, "Could not cleanly stop stream",
				slog.String("stream", spec.Name), slog.Any("error", err))
		}
	}

	if err = c.buildStream(tree); err != nil {
		if exists {
			c.restoreStream(old.tree.Spec)
		}
		return nil, errors.Join(domain.ErrStreamBuildFailed, err)
	}

	c.runStream(tree, 0)
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

	c.logger.InfoContext(ctx, "Stopping stream", slog.String("stream", name))
	// Every component has been asked to stop even if some failed, so the
	// stream is removed either way.
	if err := c.stopStream(stream); err != nil {
		c.logger.ErrorContext(ctx, "Could not cleanly stop stream",
			slog.String("stream", name), slog.Any("error", err))
	}

	delete(c.streams, name)
	c.notify(domain.StreamSpecWatchEventTypeDelete, stream.tree.Spec)
	c.logger.InfoContext(ctx, "Deleted stream", slog.String("stream", name))

	return nil
}

func (c *Core) GetStream(_ context.Context, name string) (*domain.StreamSpec, error) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	stream, exists := c.streams[name]
	if !exists {
		return nil, domain.ErrStreamNotFound
	}
	return &stream.tree.Spec, nil
}

func (c *Core) GetStreams(_ context.Context) ([]domain.StreamSpec, error) {
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
		c.logger.InfoContext(c.ctx, "Stopping stream", slog.String("stream", name))
		if err := c.stopStream(stream); err != nil {
			c.logger.ErrorContext(c.ctx, "Could not stop stream", slog.String("stream", name), slog.Any("error", err))
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
