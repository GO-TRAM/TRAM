// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package domain

import (
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/protobuf/proto"
)

// MetricPart is a metric, or a copy of it with some of its data points, whose
// data points share a key.
type MetricPart struct {
	Key    string
	Metric *metricspb.Metric
}

// AppendMetricParts appends the parts of m to dst: one per distinct key of its
// data points, in the order the keys first appear. A part holds its data
// points in their original order, and m's name, description, unit, metadata
// and the fields of its data type. If all data points share a key, m itself is
// the only part, so a caller that reuses dst allocates nothing for it. key is
// called once with nil for a metric without data points.
func AppendMetricParts(
	dst []MetricPart,
	m *metricspb.Metric,
	key func(dataPoint proto.Message) (string, error),
) ([]MetricPart, error) {
	switch data := m.GetData().(type) {
	case *metricspb.Metric_Gauge:
		return appendParts(dst, m, data.Gauge.GetDataPoints(), key, gaugeOf(m))
	case *metricspb.Metric_Sum:
		return appendParts(dst, m, data.Sum.GetDataPoints(), key, sumOf(m, data.Sum))
	case *metricspb.Metric_Histogram:
		return appendParts(dst, m, data.Histogram.GetDataPoints(), key, histogramOf(m, data.Histogram))
	case *metricspb.Metric_ExponentialHistogram:
		return appendParts(dst, m, data.ExponentialHistogram.GetDataPoints(), key,
			exponentialHistogramOf(m, data.ExponentialHistogram))
	case *metricspb.Metric_Summary:
		return appendParts(dst, m, data.Summary.GetDataPoints(), key, summaryOf(m))
	}
	return appendWhole(dst, m, key)
}

func appendWhole(
	dst []MetricPart,
	m *metricspb.Metric,
	key func(proto.Message) (string, error),
) ([]MetricPart, error) {
	k, err := key(nil)
	if err != nil {
		return dst, err
	}
	return append(dst, MetricPart{Key: k, Metric: m}), nil
}

func appendParts[T proto.Message](
	dst []MetricPart,
	m *metricspb.Metric,
	dataPoints []T,
	key func(proto.Message) (string, error),
	build func([]T) *metricspb.Metric,
) ([]MetricPart, error) {
	if len(dataPoints) == 0 {
		return appendWhole(dst, m, key)
	}
	first, err := key(dataPoints[0])
	if err != nil {
		return dst, err
	}
	for i := 1; i < len(dataPoints); i++ {
		k, keyErr := key(dataPoints[i])
		if keyErr != nil {
			return dst, keyErr
		}
		if k != first {
			return appendSplit(dst, dataPoints, i, first, k, key, build)
		}
	}
	return append(dst, MetricPart{Key: first, Metric: m}), nil
}

// appendSplit appends the parts of a metric whose data point at index differs
// in its key from the ones before it, which all have the key first.
func appendSplit[T proto.Message](
	dst []MetricPart,
	dataPoints []T,
	index int,
	first, differing string,
	key func(proto.Message) (string, error),
	build func([]T) *metricspb.Metric,
) ([]MetricPart, error) {
	order := []string{first, differing}
	groups := map[string][]T{
		first:     append(make([]T, 0, len(dataPoints)), dataPoints[:index]...),
		differing: {dataPoints[index]},
	}
	for _, dp := range dataPoints[index+1:] {
		k, err := key(dp)
		if err != nil {
			return dst, err
		}
		if _, ok := groups[k]; !ok {
			order = append(order, k)
		}
		groups[k] = append(groups[k], dp)
	}
	for _, k := range order {
		dst = append(dst, MetricPart{Key: k, Metric: build(groups[k])})
	}
	return dst, nil
}
