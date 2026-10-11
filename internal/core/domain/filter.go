// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package domain

import (
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

type LogFilter func(resource *resourcepb.Resource, scope *commonpb.InstrumentationScope, record *logspb.LogRecord) (bool, error)

type MetricFilter func(resource *resourcepb.Resource, scope *commonpb.InstrumentationScope, metric *metricspb.Metric, dataPoint proto.Message) (bool, error)

type SpanFilter func(resource *resourcepb.Resource, scope *commonpb.InstrumentationScope, span *tracepb.Span) (bool, error)

// The Filter functions spell out the loops per signal instead of sharing a
// generic walk over the tree: every level of indirection is paid per record,
// and a generic version was 10-25% slower in BenchmarkFilter. For the same
// reason, resource and scope are read once per container, not per record.

//nolint:dupl // Same shape for another OTLP type; kept apart for speed, see above.
func FilterLogs(data *logspb.LogsData, keep LogFilter) (*logspb.LogsData, error) {
	if data == nil {
		return data, nil
	}
	resources, unchanged, err := prune(
		data.GetResourceLogs(),
		func(rl *logspb.ResourceLogs) (*logspb.ResourceLogs, error) {
			resource := rl.GetResource()
			scopes, scopesUnchanged, err := prune(
				rl.GetScopeLogs(),
				func(sl *logspb.ScopeLogs) (*logspb.ScopeLogs, error) {
					scope := sl.GetScope()
					records, recordsUnchanged, err := prune(
						sl.GetLogRecords(),
						func(record *logspb.LogRecord) (*logspb.LogRecord, error) {
							return keepIf(record)(keep(resource, scope, record))
						},
					)
					if err != nil || recordsUnchanged || len(records) == 0 {
						return keepIf(sl)(recordsUnchanged, err)
					}
					return &logspb.ScopeLogs{
						Scope:      scope,
						SchemaUrl:  sl.GetSchemaUrl(),
						LogRecords: records,
					}, nil
				},
			)
			if err != nil || scopesUnchanged || len(scopes) == 0 {
				return keepIf(rl)(scopesUnchanged, err)
			}
			return &logspb.ResourceLogs{
				Resource:  resource,
				SchemaUrl: rl.GetSchemaUrl(),
				ScopeLogs: scopes,
			}, nil
		},
	)
	if err != nil || unchanged || len(resources) == 0 {
		return keepIf(data)(unchanged, err)
	}
	return &logspb.LogsData{ResourceLogs: resources}, nil
}

//nolint:dupl // Same shape for another OTLP type; kept apart for speed, see above.
func FilterSpans(data *tracepb.TracesData, keep SpanFilter) (*tracepb.TracesData, error) {
	if data == nil {
		return data, nil
	}
	resources, unchanged, err := prune(
		data.GetResourceSpans(),
		func(rs *tracepb.ResourceSpans) (*tracepb.ResourceSpans, error) {
			resource := rs.GetResource()
			scopes, scopesUnchanged, err := prune(
				rs.GetScopeSpans(),
				func(ss *tracepb.ScopeSpans) (*tracepb.ScopeSpans, error) {
					scope := ss.GetScope()
					spans, spansUnchanged, err := prune(ss.GetSpans(), func(span *tracepb.Span) (*tracepb.Span, error) {
						return keepIf(span)(keep(resource, scope, span))
					})
					if err != nil || spansUnchanged || len(spans) == 0 {
						return keepIf(ss)(spansUnchanged, err)
					}
					return &tracepb.ScopeSpans{Scope: scope, SchemaUrl: ss.GetSchemaUrl(), Spans: spans}, nil
				},
			)
			if err != nil || scopesUnchanged || len(scopes) == 0 {
				return keepIf(rs)(scopesUnchanged, err)
			}
			return &tracepb.ResourceSpans{
				Resource:   resource,
				SchemaUrl:  rs.GetSchemaUrl(),
				ScopeSpans: scopes,
			}, nil
		},
	)
	if err != nil || unchanged || len(resources) == 0 {
		return keepIf(data)(unchanged, err)
	}
	return &tracepb.TracesData{ResourceSpans: resources}, nil
}

func FilterMetrics(data *metricspb.MetricsData, keep MetricFilter) (*metricspb.MetricsData, error) {
	if data == nil {
		return data, nil
	}
	resources, unchanged, err := prune(
		data.GetResourceMetrics(),
		func(rm *metricspb.ResourceMetrics) (*metricspb.ResourceMetrics, error) {
			resource := rm.GetResource()
			scopes, scopesUnchanged, err := prune(
				rm.GetScopeMetrics(),
				func(sm *metricspb.ScopeMetrics) (*metricspb.ScopeMetrics, error) {
					scope := sm.GetScope()
					metrics, metricsUnchanged, err := prune(
						sm.GetMetrics(),
						func(m *metricspb.Metric) (*metricspb.Metric, error) {
							return filterDataPoints(m, func(dp proto.Message) (bool, error) {
								return keep(resource, scope, m, dp)
							})
						},
					)
					if err != nil || metricsUnchanged || len(metrics) == 0 {
						return keepIf(sm)(metricsUnchanged, err)
					}
					return &metricspb.ScopeMetrics{
						Scope:     scope,
						SchemaUrl: sm.GetSchemaUrl(),
						Metrics:   metrics,
					}, nil
				},
			)
			if err != nil || scopesUnchanged || len(scopes) == 0 {
				return keepIf(rm)(scopesUnchanged, err)
			}
			return &metricspb.ResourceMetrics{
				Resource:     resource,
				SchemaUrl:    rm.GetSchemaUrl(),
				ScopeMetrics: scopes,
			}, nil
		},
	)
	if err != nil || unchanged || len(resources) == 0 {
		return keepIf(data)(unchanged, err)
	}
	return &metricspb.MetricsData{ResourceMetrics: resources}, nil
}

func filterDataPoints(m *metricspb.Metric, keep func(proto.Message) (bool, error)) (*metricspb.Metric, error) {
	switch data := m.GetData().(type) {
	case *metricspb.Metric_Gauge:
		return filterTyped(m, data.Gauge.GetDataPoints(), keep, gaugeOf(m))
	case *metricspb.Metric_Sum:
		return filterTyped(m, data.Sum.GetDataPoints(), keep, sumOf(m, data.Sum))
	case *metricspb.Metric_Histogram:
		return filterTyped(m, data.Histogram.GetDataPoints(), keep, histogramOf(m, data.Histogram))
	case *metricspb.Metric_ExponentialHistogram:
		return filterTyped(m, data.ExponentialHistogram.GetDataPoints(), keep,
			exponentialHistogramOf(m, data.ExponentialHistogram))
	case *metricspb.Metric_Summary:
		return filterTyped(m, data.Summary.GetDataPoints(), keep, summaryOf(m))
	}
	return m, nil
}

func filterTyped[T interface {
	comparable
	proto.Message
}](m *metricspb.Metric, dataPoints []T, keep func(proto.Message) (bool, error), build func([]T) *metricspb.Metric) (
	*metricspb.Metric, error,
) {
	dps, unchanged, err := prune(dataPoints, func(dp T) (T, error) {
		return keepIf(dp)(keep(dp))
	})
	if err != nil || unchanged || len(dps) == 0 {
		return keepIf(m)(unchanged, err)
	}
	return build(dps), nil
}

// The ...Of functions return a builder for copies of m that hold other data
// points of m's type. A copy keeps m's name, description, unit and metadata,
// and the fields of its data type, like a sum's temporality.

func gaugeOf(m *metricspb.Metric) func([]*metricspb.NumberDataPoint) *metricspb.Metric {
	return func(dps []*metricspb.NumberDataPoint) *metricspb.Metric {
		out := withoutData(m)
		out.Data = &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{DataPoints: dps}}
		return out
	}
}

func sumOf(m *metricspb.Metric, sum *metricspb.Sum) func([]*metricspb.NumberDataPoint) *metricspb.Metric {
	return func(dps []*metricspb.NumberDataPoint) *metricspb.Metric {
		out := withoutData(m)
		out.Data = &metricspb.Metric_Sum{Sum: &metricspb.Sum{
			DataPoints:             dps,
			AggregationTemporality: sum.GetAggregationTemporality(),
			IsMonotonic:            sum.GetIsMonotonic(),
		}}
		return out
	}
}

func histogramOf(
	m *metricspb.Metric,
	histogram *metricspb.Histogram,
) func([]*metricspb.HistogramDataPoint) *metricspb.Metric {
	return func(dps []*metricspb.HistogramDataPoint) *metricspb.Metric {
		out := withoutData(m)
		out.Data = &metricspb.Metric_Histogram{Histogram: &metricspb.Histogram{
			DataPoints:             dps,
			AggregationTemporality: histogram.GetAggregationTemporality(),
		}}
		return out
	}
}

func exponentialHistogramOf(
	m *metricspb.Metric,
	histogram *metricspb.ExponentialHistogram,
) func([]*metricspb.ExponentialHistogramDataPoint) *metricspb.Metric {
	return func(dps []*metricspb.ExponentialHistogramDataPoint) *metricspb.Metric {
		out := withoutData(m)
		out.Data = &metricspb.Metric_ExponentialHistogram{ExponentialHistogram: &metricspb.ExponentialHistogram{
			DataPoints:             dps,
			AggregationTemporality: histogram.GetAggregationTemporality(),
		}}
		return out
	}
}

func summaryOf(m *metricspb.Metric) func([]*metricspb.SummaryDataPoint) *metricspb.Metric {
	return func(dps []*metricspb.SummaryDataPoint) *metricspb.Metric {
		out := withoutData(m)
		out.Data = &metricspb.Metric_Summary{Summary: &metricspb.Summary{DataPoints: dps}}
		return out
	}
}

func withoutData(m *metricspb.Metric) *metricspb.Metric {
	return &metricspb.Metric{
		Name:        m.GetName(),
		Description: m.GetDescription(),
		Unit:        m.GetUnit(),
		Metadata:    m.GetMetadata(),
	}
}

func keepIf[T any](v T) func(bool, error) (T, error) {
	return func(keep bool, err error) (T, error) {
		var zero T
		if err != nil || !keep {
			return zero, err
		}
		return v, nil
	}
}

func prune[T comparable](in []T, f func(T) (T, error)) ([]T, bool, error) {
	var zero T
	var out []T
	for i, v := range in {
		kept, err := f(v)
		if err != nil {
			return nil, false, err
		}
		if out == nil && kept == v {
			continue
		}
		if out == nil {
			out = make([]T, i, len(in))
			copy(out, in[:i])
		}
		if kept != zero {
			out = append(out, kept)
		}
	}
	if out == nil {
		return in, true, nil
	}
	return out, false, nil
}
