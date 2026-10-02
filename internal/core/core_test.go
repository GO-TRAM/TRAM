package core_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"codeberg.org/eljoth/tram/internal/core"
	"codeberg.org/eljoth/tram/internal/core/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const brokenType = "broken"

// registry tracks the components created by the fake factories.
type registry struct {
	mu sync.Mutex
	// liveSources counts sources that are not stopped yet. Like a listening
	// port, only one may exist at a time.
	liveSources int
	stopped     []string
}

func (r *registry) stop(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stopped = append(r.stopped, name)
}

func (r *registry) stoppedNames() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.stopped...)
}

type fakeSource struct {
	reg      *registry
	name     string
	shutdown chan struct{}
	subs     []domain.Subscriber
}

func (s *fakeSource) Name() string { return s.name }
func (s *fakeSource) Subscribe(sub domain.Subscriber) error {
	s.subs = append(s.subs, sub)
	return nil
}
func (s *fakeSource) Run(ctx context.Context) error {
	select {
	case <-s.shutdown:
	case <-ctx.Done():
	}
	return nil
}
func (s *fakeSource) Stop() error {
	close(s.shutdown)
	s.reg.mu.Lock()
	s.reg.liveSources--
	s.reg.mu.Unlock()
	s.reg.stop(s.name)
	return nil
}

type fakeSink struct {
	reg  *registry
	name string
	from string
}

func (s *fakeSink) Name() string                                { return s.name }
func (s *fakeSink) From() string                                { return s.from }
func (s *fakeSink) Notify(context.Context, *domain.Event) error { return nil }
func (s *fakeSink) Stop() error                                 { s.reg.stop(s.name); return nil }

type sourceFactory struct{ reg *registry }

func (f sourceFactory) New(spec domain.SourceSpec) (domain.Source, error) {
	f.reg.mu.Lock()
	defer f.reg.mu.Unlock()
	if spec.Type == brokenType {
		return nil, errors.New("broken source")
	}
	if f.reg.liveSources > 0 {
		return nil, domain.ErrPortUnavailable
	}
	f.reg.liveSources++
	return &fakeSource{reg: f.reg, name: spec.Name, shutdown: make(chan struct{})}, nil
}

type fakeStage struct {
	fakeSink
	subs []domain.Subscriber
}

func (s *fakeStage) Subscribe(sub domain.Subscriber) error {
	s.subs = append(s.subs, sub)
	return nil
}

type stageFactory struct{ reg *registry }

func (f stageFactory) New(spec domain.StageSpec) (domain.Stage, error) {
	return &fakeStage{fakeSink: fakeSink{reg: f.reg, name: spec.Name, from: spec.From}}, nil
}

type sinkFactory struct{ reg *registry }

func (f sinkFactory) New(spec domain.SinkSpec) (domain.Sink, error) {
	if spec.Type == brokenType {
		return nil, errors.New("broken sink")
	}
	return &fakeSink{reg: f.reg, name: spec.Name, from: spec.From}, nil
}

func newCore(t *testing.T) (*core.Core, *registry) {
	t.Helper()
	reg := &registry{}
	c := core.New(t.Context(), sourceFactory{reg}, stageFactory{reg}, sinkFactory{reg})
	t.Cleanup(func() { _ = c.Close() })
	return c, reg
}

func streamSpec(sourceName string, sinks ...domain.SinkSpec) domain.StreamSpec {
	return domain.StreamSpec{
		Name:   "stream",
		Source: domain.SourceSpec{Name: sourceName, Type: "fake"},
		Sinks:  sinks,
	}
}

func sink(name, typ string) domain.SinkSpec {
	return domain.SinkSpec{Name: name, Type: typ, From: "source"}
}

func TestUpsertStream(t *testing.T) {
	cases := []struct {
		name         string
		update       domain.StreamSpec
		expectErr    bool
		expectSource string
		expectSinks  []string
		// expectStopped lists the components stopped by the update.
		expectStopped []string
	}{
		{
			name:          "replaces stream that holds an exclusive resource",
			update:        streamSpec("source", sink("new", "fake")),
			expectSinks:   []string{"new"},
			expectStopped: []string{"source", "old"},
		},
		{
			name:        "restores old stream when building the update fails",
			update:      streamSpec("source", sink("new", brokenType)),
			expectErr:   true,
			expectSinks: []string{"old"},
			// The old stream, then the source created for the failed update.
			expectStopped: []string{"source", "old", "source"},
		},
		{
			name:        "keeps old stream running when the update is not a tree",
			update:      streamSpec("source", domain.SinkSpec{Name: "new", Type: "fake", From: "missing"}),
			expectErr:   true,
			expectSinks: []string{"old"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, reg := newCore(t)

			_, err := c.UpsertStream(t.Context(), streamSpec("source", sink("old", "fake")))
			require.NoError(t, err)

			_, err = c.UpsertStream(t.Context(), tc.update)
			if tc.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			got, err := c.GetStream(t.Context(), "stream")
			require.NoError(t, err)
			var sinks []string
			for _, s := range got.Sinks {
				sinks = append(sinks, s.Name)
			}
			assert.Equal(t, tc.expectSinks, sinks)
			assert.ElementsMatch(t, tc.expectStopped, reg.stoppedNames())

			reg.mu.Lock()
			assert.Equal(t, 1, reg.liveSources)
			reg.mu.Unlock()
		})
	}
}

// Stages like a batch flush on Stop, so they have to stop before their children.
func TestDeleteStreamStopsTopDown(t *testing.T) {
	c, reg := newCore(t)

	spec := domain.StreamSpec{
		Name:   "stream",
		Source: domain.SourceSpec{Name: "source", Type: "fake"},
		Stages: []domain.StageSpec{
			{Name: "first", Type: "fake", From: "source"},
			{Name: "second", Type: "fake", From: "first"},
		},
		Sinks: []domain.SinkSpec{{Name: "out", Type: "fake", From: "second"}},
	}
	_, err := c.UpsertStream(t.Context(), spec)
	require.NoError(t, err)
	require.NoError(t, c.DeleteStream(t.Context(), "stream"))

	assert.Equal(t, []string{"source", "first", "second", "out"}, reg.stoppedNames())
}

func TestDeleteStreamStopsAllComponents(t *testing.T) {
	c, reg := newCore(t)

	_, err := c.UpsertStream(t.Context(), streamSpec("source", sink("a", "fake"), sink("b", "fake")))
	require.NoError(t, err)
	require.NoError(t, c.DeleteStream(t.Context(), "stream"))

	assert.ElementsMatch(t, []string{"source", "a", "b"}, reg.stoppedNames())
	_, err = c.GetStream(t.Context(), "stream")
	assert.ErrorIs(t, err, domain.ErrStreamNotFound)
}

func TestFailedBuildStopsCreatedComponents(t *testing.T) {
	c, reg := newCore(t)

	_, err := c.UpsertStream(t.Context(), streamSpec("source", sink("a", "fake"), sink("b", brokenType)))
	assert.ErrorIs(t, err, domain.ErrStreamBuildFailed)

	assert.ElementsMatch(t, []string{"source", "a"}, reg.stoppedNames())
}

func TestWatch(t *testing.T) {
	c, _ := newCore(t)

	ctx, cancel := context.WithCancel(t.Context())
	ch := make(chan domain.StreamSpecWatchEvent)
	require.NoError(t, c.Watch(ctx, ch))

	done := make(chan error, 1)
	go func() {
		_, err := c.UpsertStream(t.Context(), streamSpec("source", sink("a", "fake")))
		done <- err
	}()

	select {
	case event := <-ch:
		assert.Equal(t, domain.StreamSpecWatchEventTypeUpsert, event.EventType)
		assert.Equal(t, "stream", event.StreamSpec.Name)
	case <-time.After(time.Second):
		t.Fatal("no watch event received")
	}
	require.NoError(t, <-done)

	// After the watcher is gone, changes must not block on its channel.
	cancel()
	deleted := make(chan error, 1)
	go func() { deleted <- c.DeleteStream(t.Context(), "stream") }()

	select {
	case err := <-deleted:
		assert.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("DeleteStream blocked on a cancelled watcher")
	}
}
