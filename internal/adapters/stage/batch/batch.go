package batch

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/go-playground/validator/v10"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"

	"codeberg.org/eljoth/tram/internal/adapters/stage/base"
	"codeberg.org/eljoth/tram/internal/core/domain"
)

const (
	Type = "batch"

	defaultMaxRecords = 8192
	defaultTimeout    = 200 * time.Millisecond
)

// Batch blocks Notify until the batch holding the event has been delivered, so
// sources ack exactly what was delivered and a slow downstream slows them down
// instead of growing a buffer.
type Batch struct {
	*base.Stage[Spec]

	maxRecords int
	timeout    time.Duration

	mu      sync.Mutex
	current *batch
	stopped bool
	flushes sync.WaitGroup
}

type batch struct {
	entries []*entry
	records int
	sealed  bool
	timer   *time.Timer
}

type entry struct {
	ctx       context.Context
	event     *domain.Event
	records   int
	cancelled bool
	done      chan struct{}
	err       error
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

func (b *Batch) MaxHold() time.Duration { return b.timeout }

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
		b.current = nil
		b.flushes.Add(1)
	}
	b.mu.Unlock()

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
			merged.Metrics.ResourceMetrics = append(
				merged.Metrics.ResourceMetrics,
				e.event.Metrics.GetResourceMetrics()...)
		}
		if traces > 0 {
			merged.Traces.ResourceSpans = append(merged.Traces.ResourceSpans, e.event.Traces.GetResourceSpans()...)
		}
	}
	return merged
}
