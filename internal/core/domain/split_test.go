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
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"codeberg.org/tram/tram/internal/core/domain"
)

func teamAttrs(name string) []*commonpb.KeyValue {
	return []*commonpb.KeyValue{{
		Key:   "team",
		Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: name}},
	}}
}

// dataPointTeam is the key function of the tests: the team of a data point.
func dataPointTeam(dp proto.Message) (string, error) {
	if dp == nil {
		return "none", nil
	}
	field := dp.ProtoReflect().Descriptor().Fields().ByName("attributes")
	list := dp.ProtoReflect().Get(field).List()
	attrs := make([]*commonpb.KeyValue, list.Len())
	for i := range list.Len() {
		attrs[i] = list.Get(i).Message().Interface().(*commonpb.KeyValue)
	}
	return team(attrs), nil
}

// dataPoints returns the data points of a metric of any type.
func dataPoints(m *metricspb.Metric) []proto.Message {
	var points []proto.Message
	m.ProtoReflect().Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		if field.ContainingOneof() == nil {
			return true
		}
		inner := value.Message()
		list := inner.Get(inner.Descriptor().Fields().ByName("data_points")).List()
		for i := range list.Len() {
			points = append(points, list.Get(i).Message().Interface())
		}
		return true
	})
	return points
}

// withoutDataPoints is a copy of m without its data points, to compare the
// other fields.
func withoutDataPoints(m *metricspb.Metric) *metricspb.Metric {
	out := proto.Clone(m).(*metricspb.Metric)
	out.ProtoReflect().Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		if field.ContainingOneof() != nil {
			inner := value.Message()
			inner.Clear(inner.Descriptor().Fields().ByName("data_points"))
		}
		return true
	})
	return out
}

func describedMetric() *metricspb.Metric {
	return &metricspb.Metric{
		Name:        "requests",
		Description: "handled requests",
		Unit:        "{request}",
		Metadata:    teamAttrs("owner"),
	}
}

// metricsOfEveryType returns a metric of each data type with data points of
// the teams A, B, A, C and every other field set.
func metricsOfEveryType() map[string]*metricspb.Metric {
	number := func() []*metricspb.NumberDataPoint {
		return []*metricspb.NumberDataPoint{
			{Attributes: teamAttrs("A"), TimeUnixNano: 1},
			{Attributes: teamAttrs("B"), TimeUnixNano: 2},
			{Attributes: teamAttrs("A"), TimeUnixNano: 3},
			{Attributes: teamAttrs("C"), TimeUnixNano: 4},
		}
	}
	gauge, sum, histogram, exponential, summary :=
		describedMetric(), describedMetric(), describedMetric(), describedMetric(), describedMetric()
	gauge.Data = &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{DataPoints: number()}}
	sum.Data = &metricspb.Metric_Sum{Sum: &metricspb.Sum{
		DataPoints:             number(),
		AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
		IsMonotonic:            true,
	}}
	histogram.Data = &metricspb.Metric_Histogram{Histogram: &metricspb.Histogram{
		DataPoints: []*metricspb.HistogramDataPoint{
			{Attributes: teamAttrs("A"), Count: 1},
			{Attributes: teamAttrs("B"), Count: 2},
			{Attributes: teamAttrs("A"), Count: 3},
			{Attributes: teamAttrs("C"), Count: 4},
		},
		AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA,
	}}
	exponential.Data = &metricspb.Metric_ExponentialHistogram{ExponentialHistogram: &metricspb.ExponentialHistogram{
		DataPoints: []*metricspb.ExponentialHistogramDataPoint{
			{Attributes: teamAttrs("A"), Count: 1},
			{Attributes: teamAttrs("B"), Count: 2},
			{Attributes: teamAttrs("A"), Count: 3},
			{Attributes: teamAttrs("C"), Count: 4},
		},
		AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
	}}
	summary.Data = &metricspb.Metric_Summary{Summary: &metricspb.Summary{
		DataPoints: []*metricspb.SummaryDataPoint{
			{Attributes: teamAttrs("A"), Count: 1},
			{Attributes: teamAttrs("B"), Count: 2},
			{Attributes: teamAttrs("A"), Count: 3},
			{Attributes: teamAttrs("C"), Count: 4},
		},
	}}
	return map[string]*metricspb.Metric{
		"gauge": gauge, "sum": sum, "histogram": histogram, "exponential histogram": exponential, "summary": summary,
	}
}

func TestAppendMetricPartsSplitsEveryType(t *testing.T) {
	t.Parallel()

	for name, m := range metricsOfEveryType() {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			before := proto.Clone(m)
			points := dataPoints(m)
			require.Len(t, points, 4)

			parts, err := domain.AppendMetricParts(nil, m, dataPointTeam)
			require.NoError(t, err)

			require.Len(t, parts, 3)
			assert.Equal(t, []string{"A", "B", "C"}, []string{parts[0].Key, parts[1].Key, parts[2].Key},
				"parts in the order their keys first appear")
			want := [][]proto.Message{{points[0], points[2]}, {points[1]}, {points[3]}}
			for i, part := range parts {
				assert.NotSame(t, m, part.Metric, "a split metric is copied")
				assert.True(t, proto.Equal(withoutDataPoints(m), withoutDataPoints(part.Metric)),
					"part %s keeps every field but the data points: %v", part.Key, part.Metric)
				got := dataPoints(part.Metric)
				require.Len(t, got, len(want[i]))
				for j := range got {
					assert.Same(t, want[i][j], got[j], "data points are shared, in their original order")
				}
			}
			assert.True(t, proto.Equal(before, m), "m is not changed")
		})
	}
}

func TestAppendMetricPartsKeepsUnsplitMetric(t *testing.T) {
	t.Parallel()

	m := describedMetric()
	m.Data = &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{DataPoints: []*metricspb.NumberDataPoint{
		{Attributes: teamAttrs("A")}, {Attributes: teamAttrs("A")},
	}}}

	parts, err := domain.AppendMetricParts(nil, m, dataPointTeam)
	require.NoError(t, err)
	require.Len(t, parts, 1)
	assert.Equal(t, "A", parts[0].Key)
	assert.Same(t, m, parts[0].Metric, "a metric whose data points share a key is not copied")
}

// Not parallel: AllocsPerRun measures the whole process.
//
//nolint:paralleltest // See above.
func TestAppendMetricPartsUnsplitAllocatesNothing(t *testing.T) {
	m := &metricspb.Metric{Data: &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{
		DataPoints: []*metricspb.NumberDataPoint{{Attributes: teamAttrs("A")}, {Attributes: teamAttrs("A")}},
	}}}
	key := func(proto.Message) (string, error) { return "A", nil }

	dst := make([]domain.MetricPart, 0, 1)
	allocs := testing.AllocsPerRun(100, func() {
		dst, _ = domain.AppendMetricParts(dst[:0], m, key)
	})
	assert.Zero(t, allocs, "with a reused dst, an unsplit metric allocates nothing")
}

func TestAppendMetricPartsWithoutDataPoints(t *testing.T) {
	t.Parallel()

	empty := describedMetric()
	empty.Data = &metricspb.Metric_Sum{Sum: &metricspb.Sum{}}

	for name, m := range map[string]*metricspb.Metric{"no data points": empty, "no data": describedMetric()} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			calls := 0
			parts, err := domain.AppendMetricParts(nil, m, func(dp proto.Message) (string, error) {
				calls++
				assert.Nil(t, dp)
				return "whole", nil
			})
			require.NoError(t, err)
			assert.Equal(t, 1, calls, "the key is resolved once, without a data point")
			assert.Equal(t, []domain.MetricPart{{Key: "whole", Metric: m}}, parts)
		})
	}
}

func TestAppendMetricPartsAppends(t *testing.T) {
	t.Parallel()

	existing := domain.MetricPart{Key: "earlier"}

	parts, err := domain.AppendMetricParts([]domain.MetricPart{existing}, metricsOfEveryType()["gauge"], dataPointTeam)
	require.NoError(t, err)
	require.Len(t, parts, 4)
	assert.Equal(t, existing, parts[0])
}

func TestAppendMetricPartsErrorsPropagate(t *testing.T) {
	t.Parallel()

	errKey := errors.New("no key")
	m := metricsOfEveryType()["gauge"]

	for name, failAt := range map[string]int{"first data point": 1, "after a split": 3} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			calls := 0
			_, err := domain.AppendMetricParts(nil, m, func(dp proto.Message) (string, error) {
				calls++
				if calls == failAt {
					return "", errKey
				}
				return dataPointTeam(dp)
			})
			assert.ErrorIs(t, err, errKey)
		})
	}

	_, err := domain.AppendMetricParts(nil, describedMetric(), func(proto.Message) (string, error) {
		return "", errKey
	})
	assert.ErrorIs(t, err, errKey, "without data points")
}
