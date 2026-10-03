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

// metrics records what the stages and sinks of all streams handle. The core
// wraps every subscriber it wires, so the metrics cover all component types
// without the components knowing about them.
type metrics struct {
	records  metric.Int64Counter
	duration metric.Float64Histogram
}

// newMetrics creates the instruments. An instrument that cannot be created is
// replaced by one that records nothing, and the error is returned.
func newMetrics(provider metric.MeterProvider) (*metrics, error) {
	meter := provider.Meter(meterName)

	records, recordsErr := meter.Int64Counter("tram.node.records",
		metric.WithDescription("Records a stage or sink received: log records, metric data points and spans"),
		metric.WithUnit("{record}"),
	)
	duration, durationErr := meter.Float64Histogram(
		"tram.node.duration",
		metric.WithDescription("Time a stage or sink took to handle an event, including the nodes below it"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(durationBuckets()...),
	)
	return &metrics{records: records, duration: duration}, errors.Join(recordsErr, durationErr)
}

// durationBuckets reach from a sink answering within a millisecond to a batch
// stage waiting several seconds for its flush.
func durationBuckets() []float64 {
	return []float64{0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}
}

// instrument wraps the subscriber of node, which belongs to stream, so that
// its Notify calls are measured.
func (m *metrics) instrument(stream string, node *domain.Node, subscriber domain.Subscriber) domain.Subscriber {
	base := []attribute.KeyValue{
		attribute.String("stream", stream),
		attribute.String("node", node.Name),
		attribute.String("kind", string(node.Kind)),
	}
	// The attribute sets, and the option slices passed to Add and Record,
	// are built once here instead of on every call, as Notify is on the hot
	// path.
	with := func(key, value string) metric.MeasurementOption {
		return metric.WithAttributeSet(attribute.NewSet(append(base, attribute.String(key, value))...))
	}
	add := func(signal domain.Signal) []metric.AddOption {
		return []metric.AddOption{with("signal", string(signal))}
	}
	record := func(outcome string) []metric.RecordOption {
		return []metric.RecordOption{with("outcome", outcome)}
	}
	return &instrumentedSubscriber{
		Subscriber: subscriber,
		metrics:    m,
		logs:       add(domain.SignalLogs),
		dataPoints: add(domain.SignalMetrics),
		spans:      add(domain.SignalTraces),
		ok:         record(outcomeOK),
		rejected:   record(outcomeRejected),
		failed:     record(outcomeError),
	}
}

// instrumentedSubscriber measures the Notify calls of the subscriber it
// embeds and passes everything else on unchanged.
type instrumentedSubscriber struct {
	domain.Subscriber

	metrics *metrics

	logs, dataPoints, spans []metric.AddOption
	ok, rejected, failed    []metric.RecordOption
}

func (s *instrumentedSubscriber) Notify(ctx context.Context, event *domain.Event) error {
	logs, dataPoints, spans := event.RecordCounts()
	s.add(ctx, logs, s.logs)
	s.add(ctx, dataPoints, s.dataPoints)
	s.add(ctx, spans, s.spans)

	start := time.Now()
	err := s.Subscriber.Notify(ctx, event)

	outcome := s.ok
	switch {
	case errors.Is(err, domain.ErrEventRejected):
		outcome = s.rejected
	case err != nil:
		outcome = s.failed
	}
	s.metrics.duration.Record(ctx, time.Since(start).Seconds(), outcome...)
	return err
}

// add counts records of one signal; signals without records are left out, so
// they do not show up for nodes that never see them.
func (s *instrumentedSubscriber) add(ctx context.Context, records int, signal []metric.AddOption) {
	if records > 0 {
		s.metrics.records.Add(ctx, int64(records), signal...)
	}
}
