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

func newMeteredCore(t *testing.T) (*core.Core, *registry, *sdkmetric.ManualReader) {
	t.Helper()
	c, reg := newCore(t)
	reader := sdkmetric.NewManualReader()
	require.NoError(t, c.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))))
	return c, reg, reader
}

// collect returns counter sums and histogram counts by attribute set.
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

func deliver(t *testing.T, reg *registry, event *domain.Event) error {
	t.Helper()
	subs := reg.sources[len(reg.sources)-1].subs
	require.Len(t, subs, 1, "a source has a single subscriber, its fan-out")
	return subs[0].Notify(t.Context(), event)
}

func logs(records int) *domain.Event {
	return &domain.Event{Logs: otlpgen.Logs(otlpgen.Shape{Records: records, Resources: 1, Teams: 1})}
}

func TestMetricsCountRecordsPerSignal(t *testing.T) {
	t.Parallel()

	c, reg, reader := newMeteredCore(t)
	_, err := c.UpsertStream(t.Context(), streamSpec(1, sink("sink", "fake")))
	require.NoError(t, err)

	event := &domain.Event{
		Logs:   otlpgen.Logs(otlpgen.Shape{Records: 2, Resources: 1, Teams: 1}),
		Traces: otlpgen.Traces(otlpgen.Shape{Records: 3, Resources: 1, Teams: 1}),
	}
	require.NoError(t, deliver(t, reg, event))
	require.NoError(t, deliver(t, reg, event))

	assert.Equal(t, map[attribute.Distinct]int64{
		attrs("source", "source", signal(domain.SignalLogs)):   4,
		attrs("source", "source", signal(domain.SignalTraces)): 6,
		attrs("sink", "sink", signal(domain.SignalLogs)):       4,
		attrs("sink", "sink", signal(domain.SignalTraces)):     6,
	}, collect(t, reader, "tram.node.records"), "signals without records are left out")
	assert.Equal(t, map[attribute.Distinct]int64{
		attrs("source", "source", outcome("ok")): 2,
		attrs("sink", "sink", outcome("ok")):     2,
	}, collect(t, reader, "tram.node.duration"))
}

func TestMetricsCountSourceOncePerEvent(t *testing.T) {
	t.Parallel()

	c, reg, reader := newMeteredCore(t)
	_, err := c.UpsertStream(t.Context(), streamSpec(1, sink("a", "fake"), sink("b", "fake"), sink("c", "fake")))
	require.NoError(t, err)

	require.NoError(t, deliver(t, reg, logs(2)))

	assert.Equal(t, map[attribute.Distinct]int64{
		attrs("source", "source", signal(domain.SignalLogs)): 2,
		attrs("a", "sink", signal(domain.SignalLogs)):        2,
		attrs("b", "sink", signal(domain.SignalLogs)):        2,
		attrs("c", "sink", signal(domain.SignalLogs)):        2,
	}, collect(t, reader, "tram.node.records"))
}

// One failing branch fails the event for the client.
func TestMetricsRecordOutcome(t *testing.T) {
	t.Parallel()

	cases := []struct {
		sinkType string
		outcome  string
	}{
		{sinkType: rejectingType, outcome: "rejected"},
		{sinkType: failingType, outcome: "error"},
	}

	for _, tc := range cases {
		t.Run(tc.outcome, func(t *testing.T) {
			t.Parallel()

			c, reg, reader := newMeteredCore(t)
			_, err := c.UpsertStream(t.Context(), streamSpec(1, sink("ok", "fake"), sink("broken", tc.sinkType)))
			require.NoError(t, err)

			require.Error(t, deliver(t, reg, logs(1)))

			assert.Equal(t, map[attribute.Distinct]int64{
				attrs("source", "source", outcome(tc.outcome)): 1,
				attrs("ok", "sink", outcome("ok")):             1,
				attrs("broken", "sink", outcome(tc.outcome)):   1,
			}, collect(t, reader, "tram.node.duration"))
		})
	}
}

func TestMetricsCoverStages(t *testing.T) {
	t.Parallel()

	c, reg, reader := newMeteredCore(t)
	spec := streamSpec(1, domain.SinkSpec{Name: "sink", Type: "fake", From: "stage"})
	spec.Stages = []domain.StageSpec{{Name: "stage", Type: "fake", From: "source"}}
	_, err := c.UpsertStream(t.Context(), spec)
	require.NoError(t, err)

	require.NoError(t, deliver(t, reg, logs(1)))

	// The fake stage swallows events.
	assert.Equal(t, map[attribute.Distinct]int64{
		attrs("source", "source", signal(domain.SignalLogs)): 1,
		attrs("stage", "stage", signal(domain.SignalLogs)):   1,
	}, collect(t, reader, "tram.node.records"))
}

func TestMetricsSurviveReplacedReceivers(t *testing.T) {
	t.Parallel()

	c, reg, reader := newMeteredCore(t)
	_, err := c.UpsertStream(t.Context(), streamSpec(1, sink("old", "fake")))
	require.NoError(t, err)

	// Same source, so only the receivers are replaced.
	_, err = c.UpsertStream(t.Context(), streamSpec(1, sink("new", "fake")))
	require.NoError(t, err)
	require.Equal(t, 1, reg.sourceCount(), "source must keep running")

	require.NoError(t, deliver(t, reg, logs(1)))

	assert.Equal(t, map[attribute.Distinct]int64{
		attrs("source", "source", signal(domain.SignalLogs)): 1,
		attrs("new", "sink", signal(domain.SignalLogs)):      1,
	}, collect(t, reader, "tram.node.records"))
}

func TestMetricsCountRecordsOut(t *testing.T) {
	t.Parallel()

	c, reg, reader := newMeteredCore(t)
	spec := streamSpec(1,
		domain.SinkSpec{Name: "a", Type: "fake", From: "forward"},
		domain.SinkSpec{Name: "b", Type: "fake", From: "forward"},
		domain.SinkSpec{Name: "c", Type: "fake", From: "swallow"},
	)
	spec.Stages = []domain.StageSpec{
		{Name: "forward", Type: forwardingType, From: "source"},
		{Name: "swallow", Type: "fake", From: "source"},
	}
	_, err := c.UpsertStream(t.Context(), spec)
	require.NoError(t, err)

	require.NoError(t, deliver(t, reg, logs(2)))

	assert.Equal(t, map[attribute.Distinct]int64{
		attrs("source", "source", signal(domain.SignalLogs)): 2,
		attrs("forward", "stage", signal(domain.SignalLogs)): 2,
	}, collect(t, reader, "tram.node.records.out"), "a stage that passes nothing on has no output")
	assert.Equal(t, map[attribute.Distinct]int64{
		attrs("source", "source", signal(domain.SignalLogs)): 2,
		attrs("forward", "stage", signal(domain.SignalLogs)): 2,
		attrs("swallow", "stage", signal(domain.SignalLogs)): 2,
		attrs("a", "sink", signal(domain.SignalLogs)):        2,
		attrs("b", "sink", signal(domain.SignalLogs)):        2,
	}, collect(t, reader, "tram.node.records"))
}
