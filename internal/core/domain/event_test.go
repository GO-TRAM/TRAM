// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package domain_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"

	"codeberg.org/eljoth/tram/internal/core/domain"
	"codeberg.org/eljoth/tram/internal/testutil/otlpgen"
)

func TestOTLPDataRoundTrip(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		event  *domain.Event
		signal domain.Signal
	}{
		{
			name: "logs",
			event: &domain.Event{
				Logs: &logspb.LogsData{
					ResourceLogs: []*logspb.ResourceLogs{
						{ScopeLogs: []*logspb.ScopeLogs{{LogRecords: []*logspb.LogRecord{{SeverityText: "INFO"}}}}},
					},
				},
			},
			signal: domain.SignalLogs,
		},
		{
			name: "metrics",
			event: &domain.Event{
				Metrics: &metricspb.MetricsData{
					ResourceMetrics: []*metricspb.ResourceMetrics{
						{ScopeMetrics: []*metricspb.ScopeMetrics{{Metrics: []*metricspb.Metric{{Name: "requests"}}}}},
					},
				},
			},
			signal: domain.SignalMetrics,
		},
		{
			name: "traces",
			event: &domain.Event{
				Traces: &tracepb.TracesData{
					ResourceSpans: []*tracepb.ResourceSpans{
						{
							ScopeSpans: []*tracepb.ScopeSpans{
								{
									Scope: &commonpb.InstrumentationScope{Name: "scope"},
									Spans: []*tracepb.Span{{Name: "span"}},
								},
							},
						},
					},
				},
			},
			signal: domain.SignalTraces,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

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
	t.Parallel()

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
			name: "all signals",
			event: &domain.Event{
				Logs:    otlpgen.Logs(shape),
				Metrics: otlpgen.Metrics(shape),
				Traces:  otlpgen.Traces(shape),
			},
			expected: 90,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, c.expected, c.event.RecordCount())
		})
	}
}

func TestRecordCounts(t *testing.T) {
	t.Parallel()

	event := &domain.Event{
		Logs:    otlpgen.Logs(otlpgen.Shape{Records: 10, Resources: 1, Teams: 1}),
		Metrics: otlpgen.Metrics(otlpgen.Shape{Records: 20, Resources: 2, Teams: 1}),
		Traces:  otlpgen.Traces(otlpgen.Shape{Records: 30, Resources: 3, Teams: 1}),
	}

	logs, metrics, traces := event.RecordCounts()
	assert.Equal(t, 10, logs)
	assert.Equal(t, 20, metrics)
	assert.Equal(t, 30, traces)

	var nilEvent *domain.Event
	logs, metrics, traces = nilEvent.RecordCounts()
	assert.Zero(t, logs+metrics+traces)
}

func TestMarshalOTLPPayloads(t *testing.T) {
	t.Parallel()

	shape := otlpgen.Shape{Records: 30, Resources: 3, Teams: 1}

	t.Run("all signals", func(t *testing.T) {
		t.Parallel()

		event := &domain.Event{
			Logs:    otlpgen.Logs(shape),
			Metrics: otlpgen.Metrics(shape),
			Traces:  otlpgen.Traces(shape),
		}

		payloads, err := event.MarshalOTLPPayloads()
		require.NoError(t, err)
		require.Len(t, payloads, 3)

		records := 0
		for i, signal := range []domain.Signal{domain.SignalLogs, domain.SignalMetrics, domain.SignalTraces} {
			assert.Equal(t, signal, payloads[i].Signal)
			got, unmarshalErr := domain.UnmarshalOTLPData(payloads[i].Signal, payloads[i].Data)
			require.NoError(t, unmarshalErr)
			records += got.RecordCount()
		}
		assert.Equal(t, event.RecordCount(), records)
	})

	t.Run("single signal", func(t *testing.T) {
		t.Parallel()

		payloads, err := (&domain.Event{Traces: otlpgen.Traces(shape)}).MarshalOTLPPayloads()
		require.NoError(t, err)
		require.Len(t, payloads, 1)
		assert.Equal(t, domain.SignalTraces, payloads[0].Signal)
	})

	t.Run("no signal", func(t *testing.T) {
		t.Parallel()

		_, err := (&domain.Event{}).MarshalOTLPPayloads()
		assert.Error(t, err)
	})
}

func TestMarshalOTLPDataRejectsSeveralSignals(t *testing.T) {
	t.Parallel()

	shape := otlpgen.Shape{Records: 1, Resources: 1, Teams: 1}
	event := &domain.Event{Logs: otlpgen.Logs(shape), Traces: otlpgen.Traces(shape)}

	_, _, err := event.MarshalOTLPData()
	assert.Error(t, err)
}

func TestUnmarshalOTLPDataUnknownSignal(t *testing.T) {
	t.Parallel()

	_, err := domain.UnmarshalOTLPData("", nil)
	assert.Error(t, err)
}

// TestEventReferenceSharing tests that events are passed by reference and not copied
func TestEventReferenceSharing(t *testing.T) {
	t.Parallel()

	// Create an event with logs
	shape := otlpgen.Shape{Records: 5, Resources: 2, Teams: 1}
	originalEvent := &domain.Event{
		Logs: otlpgen.Logs(shape),
	}

	// Make a copy of the event reference
	copiedEvent := originalEvent

	// Verify that both references point to the same underlying data
	assert.Equal(t, originalEvent, copiedEvent, "Copied event should be identical to original")
	assert.True(t, proto.Equal(originalEvent.Logs, copiedEvent.Logs), "Log data should be identical")

	// Verify record counts are the same
	assert.Equal(t, originalEvent.RecordCount(), copiedEvent.RecordCount(), "Record counts should match")
}

// TestEventReferenceSharingWithModification tests that modifications to event data are visible across references
func TestEventReferenceSharingWithModification(t *testing.T) {
	t.Parallel()

	// Create an event with logs
	shape := otlpgen.Shape{Records: 3, Resources: 1, Teams: 1}
	originalEvent := &domain.Event{
		Logs: otlpgen.Logs(shape),
	}

	// Create multiple references to the same event
	reference1 := originalEvent
	reference2 := originalEvent
	reference3 := originalEvent

	// Verify all references have the same record count initially
	assert.Equal(t, 3, originalEvent.RecordCount())
	assert.Equal(t, 3, reference1.RecordCount())
	assert.Equal(t, 3, reference2.RecordCount())
	assert.Equal(t, 3, reference3.RecordCount())
}

// TestEventReferenceSharingInFanout tests event reference sharing in a fanout scenario
func TestEventReferenceSharingInFanout(t *testing.T) {
	t.Parallel()

	// Create an event with logs
	shape := otlpgen.Shape{Records: 7, Resources: 1, Teams: 1}
	originalEvent := &domain.Event{
		Logs: otlpgen.Logs(shape),
	}

	// Simulate fanout by creating multiple references
	subscriberEvents := make([]*domain.Event, 3)
	for i := range subscriberEvents {
		subscriberEvents[i] = originalEvent
	}

	// Verify all subscriber events reference the same data
	for _, event := range subscriberEvents {
		assert.Equal(t, originalEvent, event, "All subscriber events should reference the original")
		assert.Equal(t, originalEvent.RecordCount(), event.RecordCount(), "Record counts should match")
	}
}

// TestEventReferenceSharingWithNilEvent tests reference sharing with nil events
func TestEventReferenceSharingWithNilEvent(t *testing.T) {
	t.Parallel()

	var nilEvent *domain.Event
	copiedEvent := nilEvent

	// Both should be nil
	assert.Nil(t, nilEvent)
	assert.Nil(t, copiedEvent)

	// Record count should be 0 for nil events
	assert.Equal(t, 0, nilEvent.RecordCount())
	assert.Equal(t, 0, copiedEvent.RecordCount())
}

// TestEventReferenceSharingWithEmptyEvent tests reference sharing with empty events
func TestEventReferenceSharingWithEmptyEvent(t *testing.T) {
	t.Parallel()

	emptyEvent := &domain.Event{}
	copiedEvent := emptyEvent

	// Both should be non-nil but empty
	assert.NotNil(t, emptyEvent)
	assert.NotNil(t, copiedEvent)

	// Record count should be 0 for empty events
	assert.Equal(t, 0, emptyEvent.RecordCount())
	assert.Equal(t, 0, copiedEvent.RecordCount())
}

// TestEventReferenceSharingWithMultipleSignals tests reference sharing with events containing multiple signals
func TestEventReferenceSharingWithMultipleSignals(t *testing.T) {
	t.Parallel()

	logsShape := otlpgen.Shape{Records: 10, Resources: 1, Teams: 1}
	metricsShape := otlpgen.Shape{Records: 15, Resources: 1, Teams: 1}
	tracesShape := otlpgen.Shape{Records: 20, Resources: 1, Teams: 1}

	originalEvent := &domain.Event{
		Logs:    otlpgen.Logs(logsShape),
		Metrics: otlpgen.Metrics(metricsShape),
		Traces:  otlpgen.Traces(tracesShape),
	}

	// Create multiple references
	reference1 := originalEvent
	reference2 := originalEvent

	// Verify all references have the same data
	assert.Equal(t, originalEvent.RecordCount(), reference1.RecordCount())
	assert.Equal(t, originalEvent.RecordCount(), reference2.RecordCount())

	// Verify individual signal counts match
	logs1, metrics1, traces1 := reference1.RecordCounts()
	logs2, metrics2, traces2 := reference2.RecordCounts()
	assert.Equal(t, logs1, logs2)
	assert.Equal(t, metrics1, metrics2)
	assert.Equal(t, traces1, traces2)
}

// TestEventReferenceSharingConcurrency tests that event references work correctly in concurrent scenarios
func TestEventReferenceSharingConcurrency(t *testing.T) {
	t.Parallel()

	// Create an event with logs
	shape := otlpgen.Shape{Records: 100, Resources: 5, Teams: 2}
	originalEvent := &domain.Event{
		Logs: otlpgen.Logs(shape),
	}

	// Create multiple references (simulating concurrent subscribers)
	subscriberEvents := make([]*domain.Event, 10)
	for i := range subscriberEvents {
		subscriberEvents[i] = originalEvent
	}

	// Verify all references point to the same event
	for i, event := range subscriberEvents {
		assert.Equal(t, originalEvent, event, "Subscriber %d should reference the original event", i)
		assert.Equal(t, originalEvent.RecordCount(), event.RecordCount(), "Record count should match for subscriber %d", i)
	}
}
