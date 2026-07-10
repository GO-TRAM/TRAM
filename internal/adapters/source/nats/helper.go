package nats

import (
	"fmt"

	"codeberg.org/eljoth/tram/internal/core/domain"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

func parseOTLPData(data []byte) (*domain.Event, error) {
	event := &domain.Event{}

	logs := &logspb.LogsData{}
	if err := proto.Unmarshal(data, logs); err == nil {
		event.Logs = logs
		return event, nil
	}

	metrics := &metricspb.MetricsData{}
	if err := proto.Unmarshal(data, metrics); err == nil {
		event.Metrics = metrics
		return event, nil
	}

	traces := &tracepb.TracesData{}
	if err := proto.Unmarshal(data, traces); err == nil {
		event.Traces = traces
		return event, nil
	}

	return nil, fmt.Errorf("unknown OTLP data type")
}
