package domain

import (
	"fmt"

	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

type Event struct {
	Logs    *logspb.LogsData
	Metrics *metricspb.MetricsData
	Traces  *tracepb.TracesData
}

func (e *Event) MarshalOTLPData() ([]byte, error) {
	if e == nil {
		return nil, fmt.Errorf("event is nil")
	}

	if e.Logs != nil {
		return proto.Marshal(e.Logs)
	}

	if e.Metrics != nil {
		return proto.Marshal(e.Metrics)
	}

	if e.Traces != nil {
		return proto.Marshal(e.Traces)
	}

	return nil, fmt.Errorf("no OTLP data type set in event (logs, metrics, or traces)")
}

func UnmarshalOTLPData(data []byte) (*Event, error) {
	event := &Event{}

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
