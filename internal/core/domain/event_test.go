package domain_test

import (
	"testing"

	"codeberg.org/eljoth/tram/internal/core/domain"
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

func TestUnmarshalOTLPDataUnknownSignal(t *testing.T) {
	_, err := domain.UnmarshalOTLPData("", nil)
	assert.Error(t, err)
}
