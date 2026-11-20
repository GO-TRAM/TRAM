// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package domain_test

import (
	"fmt"
	"slices"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"

	"github.com/go-tram/tram/internal/core/domain"
	"github.com/go-tram/tram/internal/testutil/otlpgen"
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

// An event is shared by reference between the branches of a stream, which
// read it at the same time. Reading must not write to it: run with -race.
func TestEventCanBeReadConcurrently(t *testing.T) {
	t.Parallel()

	shape := otlpgen.Shape{Records: 20, Resources: 3, Teams: 2}
	event := &domain.Event{
		Logs:    otlpgen.Logs(shape),
		Metrics: otlpgen.Metrics(shape),
		Traces:  otlpgen.Traces(shape),
	}
	want := &domain.Event{
		Logs:    proto.Clone(event.Logs).(*logspb.LogsData),
		Metrics: proto.Clone(event.Metrics).(*metricspb.MetricsData),
		Traces:  proto.Clone(event.Traces).(*tracepb.TracesData),
	}

	wantLogs, wantMetrics, wantTraces := want.RecordCounts()
	require.Positive(t, wantLogs*wantMetrics*wantTraces)

	const readers = 8
	errs := make(chan error, readers)
	var wg sync.WaitGroup
	for range readers {
		wg.Go(func() { errs <- readRepeatedly(event, want, []int{wantLogs, wantMetrics, wantTraces}) })
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	assert.True(t, proto.Equal(want.Logs, event.Logs), "reading leaves the logs unchanged")
	assert.True(t, proto.Equal(want.Metrics, event.Metrics), "reading leaves the metrics unchanged")
	assert.True(t, proto.Equal(want.Traces, event.Traces), "reading leaves the traces unchanged")
}

// readRepeatedly reads event like a branch does and compares what it reads
// with want.
func readRepeatedly(event, want *domain.Event, counts []int) error {
	for range 20 {
		logs, metrics, traces := event.RecordCounts()
		if got := []int{logs, metrics, traces}; !slices.Equal(counts, got) {
			return fmt.Errorf("record counts %v, want %v", got, counts)
		}
		payloads, err := event.MarshalOTLPPayloads()
		if err != nil {
			return err
		}
		if len(payloads) != len(counts) {
			return fmt.Errorf("%d payloads, want one per signal", len(payloads))
		}
		for _, p := range payloads {
			decoded, decodeErr := domain.UnmarshalOTLPData(p.Signal, p.Data)
			if decodeErr != nil {
				return decodeErr
			}
			if !sameSignal(want, decoded) {
				return fmt.Errorf("the %s payload decodes to other data", p.Signal)
			}
		}
	}
	return nil
}

// sameSignal reports whether the one signal decoded holds equals it in want.
func sameSignal(want, decoded *domain.Event) bool {
	switch {
	case decoded.Logs != nil:
		return proto.Equal(want.Logs, decoded.Logs)
	case decoded.Metrics != nil:
		return proto.Equal(want.Metrics, decoded.Metrics)
	case decoded.Traces != nil:
		return proto.Equal(want.Traces, decoded.Traces)
	}
	return false
}
