// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

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

	// Request is set by sources that decode events from requests. Stages that
	// build new events, like batch, do not carry it over.
	Request *Request
}

// Request describes the request an event was decoded from.
type Request struct {
	// Encoding is the content encoding of the body, e.g. "gzip" or "identity".
	Encoding string
	// WireBytes is the size of the body as received, DecodedBytes its size
	// after the content encoding was undone.
	WireBytes, DecodedBytes int
}

func (e *Event) RecordCount() int {
	logs, metrics, traces := e.RecordCounts()
	return logs + metrics + traces
}

func (e *Event) RecordCounts() (int, int, int) {
	if e == nil {
		return 0, 0, 0
	}

	var logs, metrics, traces int

	for _, rl := range e.Logs.GetResourceLogs() {
		for _, sl := range rl.GetScopeLogs() {
			logs += len(sl.GetLogRecords())
		}
	}
	for _, rm := range e.Metrics.GetResourceMetrics() {
		for _, sm := range rm.GetScopeMetrics() {
			for _, m := range sm.GetMetrics() {
				metrics += dataPointCount(m)
			}
		}
	}
	for _, rs := range e.Traces.GetResourceSpans() {
		for _, ss := range rs.GetScopeSpans() {
			traces += len(ss.GetSpans())
		}
	}
	return logs, metrics, traces
}

func dataPointCount(m *metricspb.Metric) int {
	switch data := m.GetData().(type) {
	case *metricspb.Metric_Gauge:
		return len(data.Gauge.GetDataPoints())
	case *metricspb.Metric_Sum:
		return len(data.Sum.GetDataPoints())
	case *metricspb.Metric_Histogram:
		return len(data.Histogram.GetDataPoints())
	case *metricspb.Metric_ExponentialHistogram:
		return len(data.ExponentialHistogram.GetDataPoints())
	case *metricspb.Metric_Summary:
		return len(data.Summary.GetDataPoints())
	default:
		return 0
	}
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

type OTLPPayload struct {
	Signal Signal
	Data   []byte
}

func (e *Event) MarshalOTLPPayloads() ([]OTLPPayload, error) {
	if e == nil {
		return nil, fmt.Errorf("event is nil")
	}

	// set is tracked separately, because a nil message stored in the
	// proto.Message interface is not nil itself.
	signals := []struct {
		signal Signal
		msg    proto.Message
		set    bool
	}{
		{signal: SignalLogs, msg: e.Logs, set: e.Logs != nil},
		{signal: SignalMetrics, msg: e.Metrics, set: e.Metrics != nil},
		{signal: SignalTraces, msg: e.Traces, set: e.Traces != nil},
	}

	var payloads []OTLPPayload
	for _, s := range signals {
		if !s.set {
			continue
		}
		data, err := proto.Marshal(s.msg)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal %s: %w", s.signal, err)
		}
		payloads = append(payloads, OTLPPayload{Signal: s.signal, Data: data})
	}
	if len(payloads) == 0 {
		return nil, fmt.Errorf("no OTLP data type set in event (logs, metrics, or traces)")
	}
	return payloads, nil
}

func (e *Event) MarshalOTLPData() (Signal, []byte, error) {
	payloads, err := e.MarshalOTLPPayloads()
	if err != nil {
		return "", nil, err
	}
	if len(payloads) > 1 {
		return "", nil, fmt.Errorf("event holds %d OTLP signals, expected one", len(payloads))
	}
	return payloads[0].Signal, payloads[0].Data, nil
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
