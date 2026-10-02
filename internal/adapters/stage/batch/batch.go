package batch

import (
	"context"
	"fmt"
	"sync"
	"time"

	"codeberg.org/eljoth/tram/internal/adapters/stage/base"
	"codeberg.org/eljoth/tram/internal/core/domain"
	"github.com/go-playground/validator/v10"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

const (
	Type = "batch"

	// The defaults match the OpenTelemetry Collector's batch processor.
	defaultMaxRecords = 8192
	defaultTimeout    = 200 * time.Millisecond
)

// Batch merges events from concurrent callers into one event.
//
// Notify blocks until the batch holding the event has been delivered and
// returns the delivery result. Sources therefore still ack or reject exactly
// what was delivered, and a slow downstream slows the sources down instead of
// growing the buffer. Batching only helps when events arrive concurrently,
// e.g. from parallel OTLP requests or several NATS workers.
type Batch struct {
	*base.Stage[Spec]
	maxRecords int
	timeout    time.Duration

	mu      sync.Mutex
	current *batch
	stopped bool
	// flushes tracks pending timers and running flushes, so Stop can wait
	// for them.
	flushes sync.WaitGroup
}

// batch is guarded by Batch.mu until it is sealed.
type batch struct {
	entries []*entry
	records int
	// sealed is set when the flush starts; no event is added or withdrawn
	// afterwards.
	sealed bool
	timer  *time.Timer
}

type entry struct {
	ctx       context.Context
	event     *domain.Event
	records   int
	cancelled bool
	// done is closed once the batch has been delivered; err holds the result.
	done chan struct{}
	err  error
}

func New(stage domain.StageSpec, validator *validator.Validate) (*Batch, error) {
	base, err := base.New[Spec](stage.Name, stage.From, stage.Spec, validator)
	if err != nil {
		return nil, err
	}

	maxRecords := base.Spec().MaxRecords
	if maxRecords == 0 {
		maxRecords = defaultMaxRecords
	}

	timeout := defaultTimeout
	if raw := base.Spec().Timeout; raw != "" {
		timeout, err = time.ParseDuration(raw)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrInvalidTimeout, err)
		}
		if timeout <= 0 {
			return nil, fmt.Errorf("%w: %s is not positive", ErrInvalidTimeout, raw)
		}
	}

	return &Batch{
		Stage:      base,
		maxRecords: maxRecords,
		timeout:    timeout,
	}, nil
}

// Notify adds the event to the current batch and waits for its delivery. If
// ctx is cancelled before the batch flushes, the event is withdrawn and not
// delivered. Once the flush has started the event is part of it, so Notify
// waits for the result instead.
func (b *Batch) Notify(ctx context.Context, event *domain.Event) error {
	records := event.RecordCount()
	if records == 0 {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	e := &entry{ctx: ctx, event: event, records: records, done: make(chan struct{})}

	b.mu.Lock()
	if b.stopped {
		b.mu.Unlock()
		return ErrBatchStopped
	}
	current := b.current
	if current == nil {
		current = b.newBatch()
	}
	current.entries = append(current.entries, e)
	current.records += records
	full := current.records >= b.maxRecords
	if full {
		// Detach the batch right away, so no caller joins it before the
		// flush below seals it.
		b.current = nil
		b.flushes.Add(1)
	}
	b.mu.Unlock()

	// The caller that fills the batch delivers it, which needs no extra
	// goroutine and lets backpressure reach the source directly.
	if full {
		b.flush(current)
		b.flushes.Done()
	}

	select {
	case <-e.done:
		return e.err
	case <-ctx.Done():
	}

	b.mu.Lock()
	sealed := current.sealed
	if !sealed {
		e.cancelled = true
		current.records -= e.records
	}
	b.mu.Unlock()

	if !sealed {
		return ctx.Err()
	}
	<-e.done
	return e.err
}

// newBatch opens a batch that flushes after the timeout. Must be called with
// b.mu held.
func (b *Batch) newBatch() *batch {
	current := &batch{}
	b.current = current
	b.flushes.Add(1)
	current.timer = time.AfterFunc(b.timeout, func() {
		defer b.flushes.Done()
		b.flush(current)
	})
	return current
}

// flush seals the batch and delivers its remaining events. Only the first
// call for a batch delivers it; later calls return immediately.
func (b *Batch) flush(current *batch) {
	b.mu.Lock()
	if current.sealed {
		b.mu.Unlock()
		return
	}
	current.sealed = true
	if b.current == current {
		b.current = nil
	}
	if current.timer.Stop() {
		// The timer will not fire anymore, so release its slot.
		b.flushes.Done()
	}

	entries := make([]*entry, 0, len(current.entries))
	for _, e := range current.entries {
		if !e.cancelled {
			entries = append(entries, e)
		}
	}
	b.mu.Unlock()

	if len(entries) == 0 {
		return
	}

	// The batch outlives every single caller, so cancelling one caller must
	// not abort the delivery for the others. The context values, like the
	// path, are the same for all callers.
	ctx := context.WithoutCancel(entries[0].ctx)
	err := b.NotifySubscribers(ctx, merge(entries))
	for _, e := range entries {
		e.err = err
		close(e.done)
	}
}

// Stop delivers the open batch and waits for running flushes. Events arriving
// afterwards are rejected with ErrBatchStopped.
func (b *Batch) Stop() error {
	b.mu.Lock()
	b.stopped = true
	current := b.current
	b.mu.Unlock()

	if current != nil {
		b.flush(current)
	}
	b.flushes.Wait()
	return b.Stage.Stop()
}

// merge combines the events into one by concatenating their resource entries
// per signal. Resources and scopes that appear in several events are not
// merged, which OTLP allows. The inputs are shared with other branches, so
// they are only referenced, never modified.
func merge(entries []*entry) *domain.Event {
	if len(entries) == 1 {
		return entries[0].event
	}

	var logs, metrics, traces int
	for _, e := range entries {
		logs += len(e.event.Logs.GetResourceLogs())
		metrics += len(e.event.Metrics.GetResourceMetrics())
		traces += len(e.event.Traces.GetResourceSpans())
	}

	merged := &domain.Event{}
	if logs > 0 {
		merged.Logs = &logspb.LogsData{ResourceLogs: make([]*logspb.ResourceLogs, 0, logs)}
	}
	if metrics > 0 {
		merged.Metrics = &metricspb.MetricsData{ResourceMetrics: make([]*metricspb.ResourceMetrics, 0, metrics)}
	}
	if traces > 0 {
		merged.Traces = &tracepb.TracesData{ResourceSpans: make([]*tracepb.ResourceSpans, 0, traces)}
	}

	for _, e := range entries {
		if logs > 0 {
			merged.Logs.ResourceLogs = append(merged.Logs.ResourceLogs, e.event.Logs.GetResourceLogs()...)
		}
		if metrics > 0 {
			merged.Metrics.ResourceMetrics = append(merged.Metrics.ResourceMetrics, e.event.Metrics.GetResourceMetrics()...)
		}
		if traces > 0 {
			merged.Traces.ResourceSpans = append(merged.Traces.ResourceSpans, e.event.Traces.GetResourceSpans()...)
		}
	}
	return merged
}
