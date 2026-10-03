package core_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"codeberg.org/eljoth/tram/internal/core"
	"codeberg.org/eljoth/tram/internal/core/domain"
	"codeberg.org/eljoth/tram/internal/testutil/otlpgen"
)

// newMeteredCore returns a core whose metrics can be read from the returned
// reader.
func newMeteredCore(t *testing.T) (*core.Core, *registry, *sdkmetric.ManualReader) {
	t.Helper()
	c, reg := newCore(t)
	reader := sdkmetric.NewManualReader()
	require.NoError(t, c.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))))
	return c, reg, reader
}

// collect returns the data points of the counter or histogram name, keyed by
// their attribute set: the record sum of a counter, the call count of a
// histogram.
func collect(t *testing.T, reader *sdkmetric.ManualReader, name string) map[attribute.Distinct]int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))

	points := map[attribute.Distinct]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			switch data := m.Data.(type) {
			case metricdata.Sum[int64]:
				for _, dp := range data.DataPoints {
					points[dp.Attributes.Equivalent()] = dp.Value
				}
			case metricdata.Histogram[float64]:
				for _, dp := range data.DataPoints {
					points[dp.Attributes.Equivalent()] = int64(dp.Count)
				}
			}
		}
	}
	return points
}

func attrs(node, kind string, kv ...attribute.KeyValue) attribute.Distinct {
	set := attribute.NewSet(append([]attribute.KeyValue{
		attribute.String("stream", "stream"),
		attribute.String("node", node),
		attribute.String("kind", kind),
	}, kv...)...)
	return set.Equivalent()
}

func signal(s domain.Signal) attribute.KeyValue { return attribute.String("signal", string(s)) }
func outcome(o string) attribute.KeyValue       { return attribute.String("outcome", o) }

func TestMetricsCountRecordsPerSignal(t *testing.T) {
	t.Parallel()

	c, reg, reader := newMeteredCore(t)
	_, err := c.UpsertStream(t.Context(), streamSpec(1, sink("sink", "fake")))
	require.NoError(t, err)

	event := &domain.Event{
		Logs:   otlpgen.Logs(otlpgen.Shape{Records: 2, Resources: 1, Teams: 1}),
		Traces: otlpgen.Traces(otlpgen.Shape{Records: 3, Resources: 1, Teams: 1}),
	}
	require.NoError(t, reg.sources[0].subs[0].Notify(t.Context(), event))
	require.NoError(t, reg.sources[0].subs[0].Notify(t.Context(), event))

	assert.Equal(t, map[attribute.Distinct]int64{
		attrs("sink", "sink", signal(domain.SignalLogs)):   4,
		attrs("sink", "sink", signal(domain.SignalTraces)): 6,
	}, collect(t, reader, "tram.node.records"), "signals without records are left out")
	assert.Equal(t, map[attribute.Distinct]int64{
		attrs("sink", "sink", outcome("ok")): 2,
	}, collect(t, reader, "tram.node.duration"))
}

func TestMetricsRecordOutcome(t *testing.T) {
	t.Parallel()

	c, reg, reader := newMeteredCore(t)
	_, err := c.UpsertStream(t.Context(), streamSpec(1,
		sink("rejecting", rejectingType),
		sink("failing", failingType),
	))
	require.NoError(t, err)

	event := &domain.Event{Logs: otlpgen.Logs(otlpgen.Shape{Records: 1, Resources: 1, Teams: 1})}
	for _, sub := range reg.sources[0].subs {
		require.Error(t, sub.Notify(t.Context(), event))
	}

	assert.Equal(t, map[attribute.Distinct]int64{
		attrs("rejecting", "sink", outcome("rejected")): 1,
		attrs("failing", "sink", outcome("error")):      1,
	}, collect(t, reader, "tram.node.duration"))
}

func TestMetricsCoverStages(t *testing.T) {
	t.Parallel()

	c, reg, reader := newMeteredCore(t)
	spec := streamSpec(1, domain.SinkSpec{Name: "sink", Type: "fake", From: "stage"})
	spec.Stages = []domain.StageSpec{{Name: "stage", Type: "fake", From: "source"}}
	_, err := c.UpsertStream(t.Context(), spec)
	require.NoError(t, err)

	event := &domain.Event{Logs: otlpgen.Logs(otlpgen.Shape{Records: 1, Resources: 1, Teams: 1})}
	require.NoError(t, reg.sources[0].subs[0].Notify(t.Context(), event))

	// The fake stage does not pass events on, so only the stage itself has
	// counted the event; the sink is wired to it but saw nothing.
	assert.Equal(t, map[attribute.Distinct]int64{
		attrs("stage", "stage", signal(domain.SignalLogs)): 1,
	}, collect(t, reader, "tram.node.records"))
}

func TestMetricsSurviveReplacedReceivers(t *testing.T) {
	t.Parallel()

	c, reg, reader := newMeteredCore(t)
	_, err := c.UpsertStream(t.Context(), streamSpec(1, sink("old", "fake")))
	require.NoError(t, err)

	// Same source, other sink: only the receivers are replaced, and the
	// running source gets the new ones through SetSubscribers.
	_, err = c.UpsertStream(t.Context(), streamSpec(1, sink("new", "fake")))
	require.NoError(t, err)
	require.Equal(t, 1, reg.sourceCount(), "source must keep running")

	event := &domain.Event{Logs: otlpgen.Logs(otlpgen.Shape{Records: 1, Resources: 1, Teams: 1})}
	require.NoError(t, reg.sources[0].subs[0].Notify(t.Context(), event))

	assert.Equal(t, map[attribute.Distinct]int64{
		attrs("new", "sink", signal(domain.SignalLogs)): 1,
	}, collect(t, reader, "tram.node.records"))
}
