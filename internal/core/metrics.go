package core

import (
	"context"
	"errors"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"codeberg.org/eljoth/tram/internal/core/domain"
)

// meterName is the instrumentation scope of the core's metrics.
const meterName = "codeberg.org/eljoth/tram/internal/core"

// Outcomes of a Notify call, recorded with its duration.
const (
	outcomeOK       = "ok"
	outcomeRejected = "rejected"
	outcomeError    = "error"
)

// metrics records what the sources, stages and sinks of all streams handle.
// The core wraps every subscriber it wires and gives every source and stage a
// fan-out to its children, so the metrics cover all component types without
// the components knowing about them.
type metrics struct {
	records    metric.Int64Counter
	recordsOut metric.Int64Counter
	duration   metric.Float64Histogram
}

// newMetrics creates the instruments. An instrument that cannot be created is
// replaced by one that records nothing, and the error is returned.
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

// durationBuckets reach from a sink answering within a millisecond to a batch
// stage waiting several seconds for its flush. They are finer in seconds, as
// a batch stage waits just over its timeout, which is usually a round number
// of seconds; with coarse buckets, its quantiles would come out close to the
// next bucket bound instead.
func durationBuckets() []float64 {
	return []float64{
		0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5,
		0.75, 1, 1.25, 1.5, 2, 2.5, 3, 4, 5, 6, 7.5, 10, 15, 30,
	}
}

// nodeAttributes builds the measurement options for node of stream with one
// more attribute. The attribute sets, and the option slices passed to Add and
// Record, are built once when wiring instead of on every call, as Notify is on
// the hot path.
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

// recordCounter counts the records of events per signal on one counter.
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

// add counts records of one signal; signals without records are left out, so
// they do not show up for nodes that never see them.
func (c *recordCounter) add(ctx context.Context, records int, signal []metric.AddOption) {
	if records > 0 {
		c.counter.Add(ctx, int64(records), signal...)
	}
}

// output returns the counter for the records node of stream passes on.
func (m *metrics) output(stream string, node *domain.Node) recordCounter {
	return newRecordCounter(m.recordsOut, nodeAttributes(stream, node))
}

// instrument wraps the subscriber of node, which belongs to stream, so that
// its Notify calls are measured.
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

// instrumentedSubscriber measures the Notify calls of the subscriber it
// embeds and passes everything else on unchanged.
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
