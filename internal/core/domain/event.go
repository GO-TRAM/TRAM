package domain

import (
	"fmt"

	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

// Event is shared by reference between all branches of a stream and delivered
// to them concurrently. Subscribers must treat it as read-only; a stage that
// wants to change it has to forward a copy.
type Event struct {
	Logs    *logspb.LogsData
	Metrics *metricspb.MetricsData
	Traces  *tracepb.TracesData
}

// Signal identifies the OTLP payload type of serialized event data. LogsData,
// MetricsData and TracesData share the same wire layout at the top level, so
// the type cannot be detected from the bytes and has to be carried alongside.
type Signal string

const (
	SignalLogs    Signal = "logs"
	SignalMetrics Signal = "metrics"
	SignalTraces  Signal = "traces"
)

// MarshalOTLPData serializes the first set payload of the event and returns
// its signal type.
func (e *Event) MarshalOTLPData() (Signal, []byte, error) {
	if e == nil {
		return "", nil, fmt.Errorf("event is nil")
	}

	var (
		signal Signal
		msg    proto.Message
	)
	switch {
	case e.Logs != nil:
		signal, msg = SignalLogs, e.Logs
	case e.Metrics != nil:
		signal, msg = SignalMetrics, e.Metrics
	case e.Traces != nil:
		signal, msg = SignalTraces, e.Traces
	default:
		return "", nil, fmt.Errorf("no OTLP data type set in event (logs, metrics, or traces)")
	}

	data, err := proto.Marshal(msg)
	if err != nil {
		return "", nil, fmt.Errorf("failed to marshal %s: %w", signal, err)
	}
	return signal, data, nil
}

func UnmarshalOTLPData(signal Signal, data []byte) (*Event, error) {
	switch signal {
	case SignalLogs:
		logs := &logspb.LogsData{}
		if err := proto.Unmarshal(data, logs); err != nil {
			return nil, fmt.Errorf("failed to unmarshal logs: %w", err)
		}
		return &Event{Logs: logs}, nil
	case SignalMetrics:
		metrics := &metricspb.MetricsData{}
		if err := proto.Unmarshal(data, metrics); err != nil {
			return nil, fmt.Errorf("failed to unmarshal metrics: %w", err)
		}
		return &Event{Metrics: metrics}, nil
	case SignalTraces:
		traces := &tracepb.TracesData{}
		if err := proto.Unmarshal(data, traces); err != nil {
			return nil, fmt.Errorf("failed to unmarshal traces: %w", err)
		}
		return &Event{Traces: traces}, nil
	default:
		return nil, fmt.Errorf("unknown OTLP signal type %q", signal)
	}
}
