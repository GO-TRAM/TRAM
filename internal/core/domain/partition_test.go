// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package domain_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"

	"github.com/go-tram/tram/internal/core/domain"
	"github.com/go-tram/tram/internal/testutil/otlpfuzz"
)

func keyOf(attrs []*commonpb.KeyValue) string {
	for _, a := range attrs {
		if a.GetKey() == otlpfuzz.KeyAttribute {
			return a.GetValue().GetStringValue()
		}
	}
	return ""
}

func logKey(_ *logspb.ResourceLogs, _ *logspb.ScopeLogs, record *logspb.LogRecord) (string, error) {
	return keyOf(record.GetAttributes()), nil
}

func spanKey(_ *tracepb.ResourceSpans, _ *tracepb.ScopeSpans, span *tracepb.Span) (string, error) {
	return keyOf(span.GetAttributes()), nil
}

func metricKey(
	_ *metricspb.ResourceMetrics, _ *metricspb.ScopeMetrics, _ *metricspb.Metric, dataPoint proto.Message,
) (string, error) {
	switch dp := dataPoint.(type) {
	case nil:
		return "no data points", nil
	case *metricspb.NumberDataPoint:
		return keyOf(dp.GetAttributes()), nil
	case *metricspb.HistogramDataPoint:
		return keyOf(dp.GetAttributes()), nil
	case *metricspb.ExponentialHistogramDataPoint:
		return keyOf(dp.GetAttributes()), nil
	case *metricspb.SummaryDataPoint:
		return keyOf(dp.GetAttributes()), nil
	}
	return "", errors.New("unknown data point type")
}

// FuzzPartition checks that the Partition functions partition any payload by
// the key of its log records, spans and data points. Plain go test runs the
// seeds; to search for failures, run:
//
//	go test -run '^$' -fuzz FuzzPartition ./internal/core/domain/
func FuzzPartition(f *testing.F) {
	for _, seed := range otlpfuzz.Seeds() {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		in := otlpfuzz.Build(data)
		var parts []otlpfuzz.Part

		logs, err := domain.PartitionLogs(in.Event.Logs, logKey)
		require.NoError(t, err)
		for _, p := range logs {
			parts = append(parts, otlpfuzz.Part{Key: p.Key, Event: &domain.Event{Logs: p.Data}})
		}
		metrics, err := domain.PartitionMetrics(in.Event.Metrics, metricKey)
		require.NoError(t, err)
		for _, p := range metrics {
			parts = append(parts, otlpfuzz.Part{Key: p.Key, Event: &domain.Event{Metrics: p.Data}})
		}
		spans, err := domain.PartitionSpans(in.Event.Traces, spanKey)
		require.NoError(t, err)
		for _, p := range spans {
			parts = append(parts, otlpfuzz.Part{Key: p.Key, Event: &domain.Event{Traces: p.Data}})
		}

		otlpfuzz.Check(t, in, parts)
	})
}

func TestPartitionWithOneKeyReturnsInput(t *testing.T) {
	t.Parallel()

	in := withEverySignal(t)

	logs, err := domain.PartitionLogs(
		in.Event.Logs,
		func(*logspb.ResourceLogs, *logspb.ScopeLogs, *logspb.LogRecord) (string, error) {
			return "all", nil
		},
	)
	require.NoError(t, err)
	require.Len(t, logs, 1)
	assert.Same(t, in.Event.Logs, logs[0].Data)

	metrics, err := domain.PartitionMetrics(in.Event.Metrics, func(
		*metricspb.ResourceMetrics, *metricspb.ScopeMetrics, *metricspb.Metric, proto.Message,
	) (string, error) {
		return "all", nil
	})
	require.NoError(t, err)
	require.Len(t, metrics, 1)
	assert.Same(t, in.Event.Metrics, metrics[0].Data, "a payload whose metrics are not split is not copied")
}

// withEverySignal returns a payload with log records, data points and spans.
func withEverySignal(t *testing.T) *otlpfuzz.Input {
	t.Helper()
	in := otlpfuzz.Build(otlpfuzz.Seeds()[8])
	logs, metrics, traces := in.Event.RecordCounts()
	require.Positive(t, logs)
	require.Positive(t, metrics)
	require.Positive(t, traces)
	return in
}

func TestPartitionWithoutEntries(t *testing.T) {
	t.Parallel()

	for name, logs := range map[string]*logspb.LogsData{
		"nil":            nil,
		"no resources":   {},
		"no log records": {ResourceLogs: []*logspb.ResourceLogs{{ScopeLogs: []*logspb.ScopeLogs{{}}}}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			parts, err := domain.PartitionLogs(logs, logKey)
			require.NoError(t, err)
			assert.Empty(t, parts)
		})
	}
}

func TestPartitionErrorsPropagate(t *testing.T) {
	t.Parallel()

	errKey := errors.New("no key")
	in := withEverySignal(t)

	_, err := domain.PartitionLogs(
		in.Event.Logs,
		func(*logspb.ResourceLogs, *logspb.ScopeLogs, *logspb.LogRecord) (string, error) {
			return "", errKey
		},
	)
	require.ErrorIs(t, err, errKey)

	_, err = domain.PartitionSpans(
		in.Event.Traces,
		func(*tracepb.ResourceSpans, *tracepb.ScopeSpans, *tracepb.Span) (string, error) {
			return "", errKey
		},
	)
	require.ErrorIs(t, err, errKey)

	_, err = domain.PartitionMetrics(in.Event.Metrics, func(
		*metricspb.ResourceMetrics, *metricspb.ScopeMetrics, *metricspb.Metric, proto.Message,
	) (string, error) {
		return "", errKey
	})
	require.ErrorIs(t, err, errKey)
}
