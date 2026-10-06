package batch_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"codeberg.org/eljoth/tram/internal/adapters/stage/batch"
	"codeberg.org/eljoth/tram/internal/core/domain"
	"codeberg.org/eljoth/tram/internal/testutil/otlpgen"
)

type recorder struct {
	mu     sync.Mutex
	events []*domain.Event
	err    error
	block  chan struct{}
}

func (r *recorder) Name() string { return "recorder" }
func (r *recorder) From() string { return "batch" }
func (r *recorder) Stop() error  { return nil }
func (r *recorder) Notify(_ context.Context, e *domain.Event) error {
	if r.block != nil {
		<-r.block
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, e)
	return r.err
}

func (r *recorder) delivered() []int {
	r.mu.Lock()
	defer r.mu.Unlock()
	counts := make([]int, len(r.events))
	for i, e := range r.events {
		counts[i] = e.RecordCount()
	}
	return counts
}

func newBatch(t *testing.T, spec map[string]any, rec *recorder) *batch.Batch {
	t.Helper()
	b, err := batch.New(domain.StageSpec{Name: "batch", From: "source", Spec: spec}, validator.New())
	require.NoError(t, err)
	require.NoError(t, b.Subscribe(rec))
	t.Cleanup(func() { _ = b.Stop() })
	return b
}

func logs(records int) *domain.Event {
	return &domain.Event{Logs: otlpgen.Logs(otlpgen.Shape{Records: records, Resources: 1, Teams: 1})}
}

func notifyAsync(ctx context.Context, b *batch.Batch, event *domain.Event) <-chan error {
	result := make(chan error, 1)
	go func() { result <- b.Notify(ctx, event) }()
	return result
}

func waitForRecords(t *testing.T, b *batch.Batch, n int) {
	t.Helper()
	require.Eventually(t, func() bool {
		records, open := batch.OpenRecords(b)
		return open && records == n
	}, time.Second, time.Millisecond)
}

func receive(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(2 * time.Second):
		t.Fatal("Notify did not return")
		return nil
	}
}

func TestNew(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name          string
		spec          map[string]any
		expectErr     bool
		expectErrIs   error
		expectMax     int
		expectTimeout time.Duration
	}{
		{name: "defaults", expectMax: batch.DefaultMaxRecords, expectTimeout: batch.DefaultTimeout},
		{
			name:          "custom",
			spec:          map[string]any{"max_records": 10, "timeout": "1s"},
			expectMax:     10,
			expectTimeout: time.Second,
		},
		{
			name:        "invalid timeout",
			spec:        map[string]any{"timeout": "soon"},
			expectErr:   true,
			expectErrIs: batch.ErrInvalidTimeout,
		},
		{
			name:        "zero timeout",
			spec:        map[string]any{"timeout": "0s"},
			expectErr:   true,
			expectErrIs: batch.ErrInvalidTimeout,
		},
		{name: "negative max records", spec: map[string]any{"max_records": -1}, expectErr: true},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			b, err := batch.New(domain.StageSpec{Name: "batch", Spec: c.spec}, validator.New())
			if c.expectErr {
				assert.Error(t, err)
				if c.expectErrIs != nil {
					require.ErrorIs(t, err, c.expectErrIs)
				}
				return
			}
			require.NoError(t, err)
			maxRecords, timeout := batch.Limits(b)
			assert.Equal(t, c.expectMax, maxRecords)
			assert.Equal(t, c.expectTimeout, timeout)
			assert.Equal(t, c.expectTimeout, b.MaxHold(), "an event waits at most the timeout")
		})
	}
}

func TestFlushOnMaxRecords(t *testing.T) {
	t.Parallel()

	rec := &recorder{}
	b := newBatch(t, map[string]any{"max_records": 5, "timeout": "1h"}, rec)

	first := notifyAsync(t.Context(), b, logs(2))
	waitForRecords(t, b, 2)
	second := notifyAsync(t.Context(), b, logs(3))

	assert.NoError(t, receive(t, first))
	assert.NoError(t, receive(t, second))
	assert.Equal(t, []int{5}, rec.delivered())
}

func TestConcurrentCallersDoNotOvershootBatches(t *testing.T) {
	t.Parallel()

	rec := &recorder{}
	b := newBatch(t, map[string]any{"max_records": 2, "timeout": "1h"}, rec)

	results := make([]<-chan error, 40)
	for i := range results {
		results[i] = notifyAsync(t.Context(), b, logs(1))
	}
	for _, result := range results {
		require.NoError(t, receive(t, result))
	}

	delivered := rec.delivered()
	assert.Len(t, delivered, 20)
	for _, records := range delivered {
		assert.Equal(t, 2, records)
	}
}

func TestFlushOnTimeout(t *testing.T) {
	t.Parallel()

	rec := &recorder{}
	b := newBatch(t, map[string]any{"max_records": 100, "timeout": "20ms"}, rec)

	start := time.Now()
	require.NoError(t, receive(t, notifyAsync(t.Context(), b, logs(3))))
	assert.GreaterOrEqual(t, time.Since(start), 20*time.Millisecond)
	assert.Equal(t, []int{3}, rec.delivered())
}

func TestEventLargerThanMaxRecordsIsFlushedWhole(t *testing.T) {
	t.Parallel()

	rec := &recorder{}
	b := newBatch(t, map[string]any{"max_records": 2, "timeout": "1h"}, rec)

	require.NoError(t, receive(t, notifyAsync(t.Context(), b, logs(10))))
	assert.Equal(t, []int{10}, rec.delivered())
}

func TestEmptyEventIsNotBatched(t *testing.T) {
	t.Parallel()

	rec := &recorder{}
	b := newBatch(t, map[string]any{"timeout": "1h"}, rec)

	require.NoError(t, b.Notify(t.Context(), &domain.Event{}))
	assert.Empty(t, rec.delivered())
}

func TestCancelBeforeFlushWithdrawsEvent(t *testing.T) {
	t.Parallel()

	rec := &recorder{}
	b := newBatch(t, map[string]any{"max_records": 100, "timeout": "100ms"}, rec)

	ctx, cancel := context.WithCancel(t.Context())
	cancelled := notifyAsync(ctx, b, logs(3))
	waitForRecords(t, b, 3)
	kept := notifyAsync(t.Context(), b, logs(2))
	waitForRecords(t, b, 5)

	cancel()
	require.ErrorIs(t, receive(t, cancelled), context.Canceled)

	require.NoError(t, receive(t, kept))
	assert.Equal(t, []int{2}, rec.delivered(), "the cancelled event must not be delivered")
}

func TestCancelledBatchIsNotDelivered(t *testing.T) {
	t.Parallel()

	rec := &recorder{}
	b := newBatch(t, map[string]any{"max_records": 100, "timeout": "20ms"}, rec)

	ctx, cancel := context.WithCancel(t.Context())
	result := notifyAsync(ctx, b, logs(3))
	waitForRecords(t, b, 3)
	cancel()

	require.ErrorIs(t, receive(t, result), context.Canceled)
	time.Sleep(50 * time.Millisecond)
	assert.Empty(t, rec.delivered())
}

func TestCancelAfterFlushStartedWaitsForResult(t *testing.T) {
	t.Parallel()

	rec := &recorder{block: make(chan struct{})}
	b := newBatch(t, map[string]any{"max_records": 2, "timeout": "1h"}, rec)

	ctx, cancel := context.WithCancel(t.Context())
	waiting := notifyAsync(ctx, b, logs(1))
	waitForRecords(t, b, 1)
	filling := notifyAsync(t.Context(), b, logs(1))

	require.Eventually(t, func() bool {
		_, open := batch.OpenRecords(b)
		return !open
	}, time.Second, time.Millisecond)
	cancel()

	select {
	case err := <-waiting:
		t.Fatalf("Notify returned %v before the delivery finished", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(rec.block)
	assert.NoError(t, receive(t, waiting))
	assert.NoError(t, receive(t, filling))
	assert.Equal(t, []int{2}, rec.delivered())
}

func TestDeliveryErrorReachesAllCallers(t *testing.T) {
	t.Parallel()

	rec := &recorder{err: domain.ErrEventRejected}
	b := newBatch(t, map[string]any{"max_records": 2, "timeout": "1h"}, rec)

	first := notifyAsync(t.Context(), b, logs(1))
	waitForRecords(t, b, 1)
	second := notifyAsync(t.Context(), b, logs(1))

	require.ErrorIs(t, receive(t, first), domain.ErrEventRejected)
	assert.ErrorIs(t, receive(t, second), domain.ErrEventRejected)
}

func TestStopDeliversOpenBatch(t *testing.T) {
	t.Parallel()

	rec := &recorder{}
	b := newBatch(t, map[string]any{"max_records": 100, "timeout": "1h"}, rec)

	result := notifyAsync(t.Context(), b, logs(3))
	waitForRecords(t, b, 3)

	require.NoError(t, b.Stop())
	assert.NoError(t, receive(t, result))
	assert.Equal(t, []int{3}, rec.delivered())

	assert.ErrorIs(t, b.Notify(t.Context(), logs(1)), batch.ErrBatchStopped)
}

func TestMerge(t *testing.T) {
	t.Parallel()

	shape := otlpgen.Shape{Records: 4, Resources: 2, Teams: 1}
	first := &domain.Event{Logs: otlpgen.Logs(shape), Traces: otlpgen.Traces(shape)}
	second := &domain.Event{Logs: otlpgen.Logs(shape), Metrics: otlpgen.Metrics(shape)}

	merged := batch.Merge(first, second)

	assert.Len(t, merged.Logs.GetResourceLogs(), 4)
	assert.Len(t, merged.Metrics.GetResourceMetrics(), 2)
	assert.Len(t, merged.Traces.GetResourceSpans(), 2)
	assert.Equal(t, first.RecordCount()+second.RecordCount(), merged.RecordCount())

	assert.Len(t, first.Logs.GetResourceLogs(), 2)
	assert.Nil(t, first.Metrics)
	assert.Len(t, second.Logs.GetResourceLogs(), 2)
	assert.Nil(t, second.Traces)
	assert.Same(t, first.Logs.GetResourceLogs()[0], merged.Logs.GetResourceLogs()[0])
}

func TestMergeSingleEventIsForwardedAsIs(t *testing.T) {
	t.Parallel()

	event := logs(3)
	assert.Same(t, event, batch.Merge(event))
}
