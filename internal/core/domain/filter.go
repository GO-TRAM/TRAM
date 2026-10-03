package domain

import (
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

// The Filter functions return a payload with only the records keep accepts,
// at the lowest level of each signal: log records, metric data points and
// spans.
//
// Events are shared and read-only, so the input is never modified. Records
// are shared with the input, and only the containers above removed records
// are rebuilt. Parts in which every record is kept are reused, so a filter
// that keeps everything returns its input without allocating. Containers left
// without records are removed, and nil is returned if no record is kept.

type LogFilter func(resource *resourcepb.Resource, scope *commonpb.InstrumentationScope, record *logspb.LogRecord) (bool, error)

// MetricFilter decides per data point. dataPoint is one of
// *metricspb.NumberDataPoint, *metricspb.HistogramDataPoint,
// *metricspb.ExponentialHistogramDataPoint or *metricspb.SummaryDataPoint.
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

// filterDataPoints returns the metric with only the data points keep accepts,
// or nil if none is kept. Metrics without data points are kept unchanged.
func filterDataPoints(m *metricspb.Metric, keep func(proto.Message) (bool, error)) (*metricspb.Metric, error) {
	switch data := m.GetData().(type) {
	case *metricspb.Metric_Gauge:
		dps, unchanged, err := pruneDataPoints(data.Gauge.GetDataPoints(), keep)
		if err != nil || unchanged || len(dps) == 0 {
			return keepIf(m)(unchanged, err)
		}
		out := withoutData(m)
		out.Data = &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{DataPoints: dps}}
		return out, nil
	case *metricspb.Metric_Sum:
		dps, unchanged, err := pruneDataPoints(data.Sum.GetDataPoints(), keep)
		if err != nil || unchanged || len(dps) == 0 {
			return keepIf(m)(unchanged, err)
		}
		out := withoutData(m)
		out.Data = &metricspb.Metric_Sum{Sum: &metricspb.Sum{
			DataPoints:             dps,
			AggregationTemporality: data.Sum.GetAggregationTemporality(),
			IsMonotonic:            data.Sum.GetIsMonotonic(),
		}}
		return out, nil
	case *metricspb.Metric_Histogram:
		dps, unchanged, err := pruneDataPoints(data.Histogram.GetDataPoints(), keep)
		if err != nil || unchanged || len(dps) == 0 {
			return keepIf(m)(unchanged, err)
		}
		out := withoutData(m)
		out.Data = &metricspb.Metric_Histogram{Histogram: &metricspb.Histogram{
			DataPoints:             dps,
			AggregationTemporality: data.Histogram.GetAggregationTemporality(),
		}}
		return out, nil
	case *metricspb.Metric_ExponentialHistogram:
		dps, unchanged, err := pruneDataPoints(data.ExponentialHistogram.GetDataPoints(), keep)
		if err != nil || unchanged || len(dps) == 0 {
			return keepIf(m)(unchanged, err)
		}
		out := withoutData(m)
		out.Data = &metricspb.Metric_ExponentialHistogram{ExponentialHistogram: &metricspb.ExponentialHistogram{
			DataPoints:             dps,
			AggregationTemporality: data.ExponentialHistogram.GetAggregationTemporality(),
		}}
		return out, nil
	case *metricspb.Metric_Summary:
		dps, unchanged, err := pruneDataPoints(data.Summary.GetDataPoints(), keep)
		if err != nil || unchanged || len(dps) == 0 {
			return keepIf(m)(unchanged, err)
		}
		out := withoutData(m)
		out.Data = &metricspb.Metric_Summary{Summary: &metricspb.Summary{DataPoints: dps}}
		return out, nil
	}
	return m, nil
}

// withoutData copies the metric's metadata for a metric with fewer data points.
func withoutData(m *metricspb.Metric) *metricspb.Metric {
	return &metricspb.Metric{
		Name:        m.GetName(),
		Description: m.GetDescription(),
		Unit:        m.GetUnit(),
		Metadata:    m.GetMetadata(),
	}
}

func pruneDataPoints[T interface {
	comparable
	proto.Message
}](dataPoints []T, keep func(proto.Message) (bool, error)) ([]T, bool, error) {
	return prune(dataPoints, func(dp T) (T, error) {
		return keepIf(dp)(keep(dp))
	})
}

// keepIf turns a keep decision into prune's convention: the element itself to
// keep it, the zero value (nil) to drop it.
func keepIf[T any](v T) func(bool, error) (T, error) {
	return func(keep bool, err error) (T, error) {
		var zero T
		if err != nil || !keep {
			return zero, err
		}
		return v, nil
	}
}

// prune rebuilds in from what f returns for each element: the element itself
// or a reduced copy to keep, or the zero value to drop it. If every element is
// kept as it is, in is returned with unchanged set, so nothing is copied.
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
