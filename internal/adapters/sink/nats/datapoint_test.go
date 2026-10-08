package nats_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/protobuf/proto"

	natssink "codeberg.org/eljoth/tram/internal/adapters/sink/nats"
	"codeberg.org/eljoth/tram/internal/core/domain"
)

const metricsByDataPoint = "'metrics.' + datapoint.attributes.get_attr('team')"

func teamAttr(name string) []*commonpb.KeyValue {
	return []*commonpb.KeyValue{{
		Key:   "team",
		Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: name}},
	}}
}

func gaugeOf(name string, teams ...string) *metricspb.Metric {
	points := make([]*metricspb.NumberDataPoint, len(teams))
	for i, team := range teams {
		points[i] = &metricspb.NumberDataPoint{Attributes: teamAttr(team), TimeUnixNano: uint64(i)}
	}
	return &metricspb.Metric{Name: name, Data: &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{DataPoints: points}}}
}

func metricsIn(service string, metrics ...*metricspb.Metric) *metricspb.ResourceMetrics {
	return &metricspb.ResourceMetrics{
		Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{attrKV("service.name", service)}},
		ScopeMetrics: []*metricspb.ScopeMetrics{
			{Scope: &commonpb.InstrumentationScope{Name: "scope"}, Metrics: metrics},
		},
	}
}

func attrKV(key, value string) *commonpb.KeyValue {
	return &commonpb.KeyValue{
		Key:   key,
		Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: value}},
	}
}

// dataPointsBySubject lists the data points of every message as
// service/metric/team, by subject.
func dataPointsBySubject(t *testing.T, messages []natssink.Message) map[string][]string {
	t.Helper()
	out := map[string][]string{}
	for _, m := range messages {
		for _, rm := range m.Event.Metrics.GetResourceMetrics() {
			service := attr(rm.GetResource().GetAttributes(), "service.name")
			for _, sm := range rm.GetScopeMetrics() {
				for _, metric := range sm.GetMetrics() {
					for _, dp := range metric.GetGauge().GetDataPoints() {
						out[m.Subject] = append(out[m.Subject],
							fmt.Sprintf("%s/%s/%s", service, metric.GetName(), attr(dp.GetAttributes(), "team")))
					}
				}
			}
		}
	}
	return out
}

func TestMetricsSplitByDataPoint(t *testing.T) {
	t.Parallel()

	mixed := gaugeOf("mixed", "A", "B", "A")
	single := gaugeOf("single", "A", "A")
	data := &metricspb.MetricsData{ResourceMetrics: []*metricspb.ResourceMetrics{
		metricsIn("first", single),
		metricsIn("second", mixed, gaugeOf("only-b", "B")),
	}}
	before := proto.Clone(data)

	messages, err := natssink.GroupEventsBySubjects(
		newSink(t, map[string]any{"metrics": metricsByDataPoint}),
		&domain.Event{Metrics: data},
	)
	require.NoError(t, err)

	assert.Equal(t, map[string][]string{
		"metrics.A": {"first/single/A", "first/single/A", "second/mixed/A", "second/mixed/A"},
		"metrics.B": {"second/mixed/B", "second/only-b/B"},
	}, dataPointsBySubject(t, messages))
	assert.Equal(t, "metrics.A", messages[0].Subject, "subjects in order of first occurrence")

	toA := messages[0].Event.Metrics.GetResourceMetrics()
	require.Len(t, toA, 2)
	assert.Same(t, single, toA[0].GetScopeMetrics()[0].GetMetrics()[0], "a metric that is not split is shared")
	assert.Same(t, data.GetResourceMetrics()[1].GetResource(), toA[1].GetResource(), "resources are shared")
	assert.NotSame(t, mixed, toA[1].GetScopeMetrics()[0].GetMetrics()[0], "a split metric is copied")
	assert.Same(
		t,
		mixed.GetGauge().GetDataPoints()[2],
		toA[1].GetScopeMetrics()[0].GetMetrics()[0].GetGauge().GetDataPoints()[1],
		"data points are shared",
	)
	assert.True(t, proto.Equal(before, data), "the input is not changed")
}

func TestMetricsWithoutDataPointVariableStayWhole(t *testing.T) {
	t.Parallel()

	mixed := gaugeOf("mixed", "A", "B")
	data := &metricspb.MetricsData{ResourceMetrics: []*metricspb.ResourceMetrics{metricsIn("svc", mixed)}}

	messages, err := natssink.GroupEventsBySubjects(
		newSink(t, map[string]any{"metrics": "'metrics.' + metric.gauge.data_points[0].attributes.get_attr('team')"}),
		&domain.Event{Metrics: data},
	)
	require.NoError(t, err)
	require.Len(t, messages, 1)
	assert.Equal(t, "metrics.A", messages[0].Subject, "routed by the first data point, as before")
	assert.Same(t, data, messages[0].Event.Metrics, "a payload with one subject is published as it is")
}

func TestDataPointVariableForEveryMetricType(t *testing.T) {
	t.Parallel()

	cases := map[string]*metricspb.Metric{
		"sum": {Data: &metricspb.Metric_Sum{Sum: &metricspb.Sum{DataPoints: []*metricspb.NumberDataPoint{
			{Attributes: teamAttr("A")}, {Attributes: teamAttr("B")},
		}}}},
		"histogram": {Data: &metricspb.Metric_Histogram{Histogram: &metricspb.Histogram{
			DataPoints: []*metricspb.HistogramDataPoint{{Attributes: teamAttr("A")}, {Attributes: teamAttr("B")}},
		}}},
		"exponential histogram": {Data: &metricspb.Metric_ExponentialHistogram{
			ExponentialHistogram: &metricspb.ExponentialHistogram{
				DataPoints: []*metricspb.ExponentialHistogramDataPoint{
					{Attributes: teamAttr("A")},
					{Attributes: teamAttr("B")},
				},
			},
		}},
		"summary": {Data: &metricspb.Metric_Summary{Summary: &metricspb.Summary{
			DataPoints: []*metricspb.SummaryDataPoint{{Attributes: teamAttr("A")}, {Attributes: teamAttr("B")}},
		}}},
	}

	for name, metric := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			messages, err := natssink.GroupEventsBySubjects(
				newSink(t, map[string]any{"metrics": metricsByDataPoint}),
				&domain.Event{Metrics: &metricspb.MetricsData{ResourceMetrics: []*metricspb.ResourceMetrics{
					metricsIn("svc", metric),
				}}},
			)
			require.NoError(t, err)
			require.Len(t, messages, 2)
			assert.Equal(t, "metrics.A", messages[0].Subject)
			assert.Equal(t, "metrics.B", messages[1].Subject)
			for _, m := range messages {
				assert.Equal(t, 1, m.Event.RecordCount())
			}
		})
	}
}

func TestMetricWithoutDataPoints(t *testing.T) {
	t.Parallel()

	event := func() *domain.Event {
		return &domain.Event{Metrics: &metricspb.MetricsData{ResourceMetrics: []*metricspb.ResourceMetrics{
			metricsIn(
				"svc",
				&metricspb.Metric{Name: "empty", Data: &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{}}},
			),
		}}}
	}

	messages, err := natssink.GroupEventsBySubjects(
		newSink(t, map[string]any{"metrics": "'metrics.' + metric.name"}),
		event(),
	)
	require.NoError(t, err)
	require.Len(t, messages, 1)
	assert.Equal(t, "metrics.empty", messages[0].Subject, "routed by an expression that does not read datapoint")

	sink := newSink(t, map[string]any{"metrics": metricsByDataPoint})
	assert.ErrorIs(t, sink.Notify(t.Context(), event()), domain.ErrEventRejected,
		"an expression that reads datapoint fails for it")
}
