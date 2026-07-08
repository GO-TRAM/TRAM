package domain

import (
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

// Event represents stream data with OTLP support
type Event struct {
	Data    []byte
	Logs    *logspb.LogsData
	Metrics *metricspb.MetricsData
	Traces  *tracepb.TracesData
}
