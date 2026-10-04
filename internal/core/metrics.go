package core

import (
	"context"
	"errors"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"codeberg.org/eljoth/tram/internal/core/domain"
)

const meterName = "codeberg.org/eljoth/tram/internal/core"

const (
	outcomeOK       = "ok"
	outcomeRejected = "rejected"
	outcomeError    = "error"
)

// metrics are recorded by the core around the components, so components need
// no instrumentation of their own.
type metrics struct {
	records    metric.Int64Counter
	recordsOut metric.Int64Counter
	duration   metric.Float64Histogram
}

// An instrument that cannot be created records nothing; the error is returned
// for logging.
func newMetrics(provider metric.MeterProvider) (*metrics, error) {
	meter := provider.Meter(meterName)

	records, recordsErr := meter.Int64Counter("tram.node.records",
		metric.WithDescription("Records a node received: log records, metric data points and spans"),
		metric.WithUnit("{record}"),
	)
	recordsOut, recordsOutErr := meter.Int64Counter("tram.node.records.out",
		metric.WithDescription(
			"Records a source or stage passed on: log records, metric data points and spans, "+
				"each counted once however many children it has",
		),
		metric.WithUnit("{record}"),
	)
	duration, durationErr := meter.Float64Histogram(
		"tram.node.duration",
		metric.WithDescription("Time a node took to handle an event, including the nodes below it"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(durationBuckets()...),
	)
	return &metrics{records: records, recordsOut: recordsOut, duration: duration},
		errors.Join(recordsErr, recordsOutErr, durationErr)
}

// Finer in seconds: a batch waits just over its timeout, usually a round
// number of seconds, and coarse buckets would push its quantiles towards the
// next bound.
func durationBuckets() []float64 {
	return []float64{
		0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5,
		0.75, 1, 1.25, 1.5, 2, 2.5, 3, 4, 5, 6, 7.5, 10, 15, 30,
	}
}

// Attribute sets and option slices are built once per node rather than per
// call, since Notify is on the hot path.
func nodeAttributes(stream string, node *domain.Node) func(key, value string) metric.MeasurementOption {
	base := []attribute.KeyValue{
		attribute.String("stream", stream),
		attribute.String("node", node.Name),
		attribute.String("kind", string(node.Kind)),
	}
	return func(key, value string) metric.MeasurementOption {
		return metric.WithAttributeSet(attribute.NewSet(append(base, attribute.String(key, value))...))
	}
}

type recordCounter struct {
	counter                 metric.Int64Counter
	logs, dataPoints, spans []metric.AddOption
}

func newRecordCounter(
	counter metric.Int64Counter,
	with func(key, value string) metric.MeasurementOption,
) recordCounter {
	add := func(signal domain.Signal) []metric.AddOption {
		return []metric.AddOption{with("signal", string(signal))}
	}
	return recordCounter{
		counter:    counter,
		logs:       add(domain.SignalLogs),
		dataPoints: add(domain.SignalMetrics),
		spans:      add(domain.SignalTraces),
	}
}

func (c *recordCounter) count(ctx context.Context, event *domain.Event) {
	logs, dataPoints, spans := event.RecordCounts()
	c.add(ctx, logs, c.logs)
	c.add(ctx, dataPoints, c.dataPoints)
	c.add(ctx, spans, c.spans)
}

// Skipping zero keeps series of signals a node never sees from appearing.
func (c *recordCounter) add(ctx context.Context, records int, signal []metric.AddOption) {
	if records > 0 {
		c.counter.Add(ctx, int64(records), signal...)
	}
}

func (m *metrics) output(stream string, node *domain.Node) recordCounter {
	return newRecordCounter(m.recordsOut, nodeAttributes(stream, node))
}

func (m *metrics) instrument(stream string, node *domain.Node, subscriber domain.Subscriber) domain.Subscriber {
	with := nodeAttributes(stream, node)
	record := func(outcome string) []metric.RecordOption {
		return []metric.RecordOption{with("outcome", outcome)}
	}
	return &instrumentedSubscriber{
		Subscriber: subscriber,
		records:    newRecordCounter(m.records, with),
		duration:   m.duration,
		ok:         record(outcomeOK),
		rejected:   record(outcomeRejected),
		failed:     record(outcomeError),
	}
}

type instrumentedSubscriber struct {
	domain.Subscriber

	records              recordCounter
	duration             metric.Float64Histogram
	ok, rejected, failed []metric.RecordOption
}

func (s *instrumentedSubscriber) Notify(ctx context.Context, event *domain.Event) error {
	s.records.count(ctx, event)

	start := time.Now()
	err := s.Subscriber.Notify(ctx, event)

	outcome := s.ok
	switch {
	case errors.Is(err, domain.ErrEventRejected):
		outcome = s.rejected
	case err != nil:
		outcome = s.failed
	}
	s.duration.Record(ctx, time.Since(start).Seconds(), outcome...)
	return err
}
