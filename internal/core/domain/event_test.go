package domain_test

import (
	"testing"

	"codeberg.org/eljoth/tram/internal/core/domain"
	"codeberg.org/eljoth/tram/internal/testutil/otlpgen"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

func TestOTLPDataRoundTrip(t *testing.T) {
	cases := []struct {
		name   string
		event  *domain.Event
		signal domain.Signal
	}{
		{
			name:   "logs",
			event:  &domain.Event{Logs: &logspb.LogsData{ResourceLogs: []*logspb.ResourceLogs{{ScopeLogs: []*logspb.ScopeLogs{{LogRecords: []*logspb.LogRecord{{SeverityText: "INFO"}}}}}}}},
			signal: domain.SignalLogs,
		},
		{
			name:   "metrics",
			event:  &domain.Event{Metrics: &metricspb.MetricsData{ResourceMetrics: []*metricspb.ResourceMetrics{{ScopeMetrics: []*metricspb.ScopeMetrics{{Metrics: []*metricspb.Metric{{Name: "requests"}}}}}}}},
			signal: domain.SignalMetrics,
		},
		{
			name:   "traces",
			event:  &domain.Event{Traces: &tracepb.TracesData{ResourceSpans: []*tracepb.ResourceSpans{{ScopeSpans: []*tracepb.ScopeSpans{{Scope: &commonpb.InstrumentationScope{Name: "scope"}, Spans: []*tracepb.Span{{Name: "span"}}}}}}}},
			signal: domain.SignalTraces,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			signal, data, err := c.event.MarshalOTLPData()
			require.NoError(t, err)
			assert.Equal(t, c.signal, signal)

			got, err := domain.UnmarshalOTLPData(signal, data)
			require.NoError(t, err)
			assert.True(t, proto.Equal(c.event.Logs, got.Logs), "logs differ")
			assert.True(t, proto.Equal(c.event.Metrics, got.Metrics), "metrics differ")
			assert.True(t, proto.Equal(c.event.Traces, got.Traces), "traces differ")
		})
	}
}

func TestRecordCount(t *testing.T) {
	shape := otlpgen.Shape{Records: 30, Resources: 3, Teams: 1}
	histogram := &metricspb.Metric{Data: &metricspb.Metric_Histogram{Histogram: &metricspb.Histogram{
		DataPoints: []*metricspb.HistogramDataPoint{{}, {}},
	}}}

	cases := []struct {
		name     string
		event    *domain.Event
		expected int
	}{
		{name: "nil", event: nil, expected: 0},
		{name: "empty", event: &domain.Event{}, expected: 0},
		{name: "logs", event: &domain.Event{Logs: otlpgen.Logs(shape)}, expected: 30},
		{name: "metric data points", event: &domain.Event{Metrics: otlpgen.Metrics(shape)}, expected: 30},
		{name: "spans", event: &domain.Event{Traces: otlpgen.Traces(shape)}, expected: 30},
		{
			name: "data points of all metric types",
			event: &domain.Event{Metrics: &metricspb.MetricsData{ResourceMetrics: []*metricspb.ResourceMetrics{{
				ScopeMetrics: []*metricspb.ScopeMetrics{{Metrics: []*metricspb.Metric{histogram, {}}}},
			}}}},
			expected: 2,
		},
		{
			name:     "all signals",
			event:    &domain.Event{Logs: otlpgen.Logs(shape), Metrics: otlpgen.Metrics(shape), Traces: otlpgen.Traces(shape)},
			expected: 90,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.expected, c.event.RecordCount())
		})
	}
}

func TestUnmarshalOTLPDataUnknownSignal(t *testing.T) {
	_, err := domain.UnmarshalOTLPData("", nil)
	assert.Error(t, err)
}
