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

func FilterLogs(data *logspb.LogsData, keep LogFilter) (*logspb.LogsData, error) {
	if data == nil {
		return nil, nil
	}
	resources, unchanged, err := prune(data.ResourceLogs, func(rl *logspb.ResourceLogs) (*logspb.ResourceLogs, error) {
		scopes, unchanged, err := prune(rl.ScopeLogs, func(sl *logspb.ScopeLogs) (*logspb.ScopeLogs, error) {
			records, unchanged, err := prune(sl.LogRecords, func(record *logspb.LogRecord) (*logspb.LogRecord, error) {
				return keepIf(record)(keep(rl.Resource, sl.Scope, record))
			})
			if err != nil || unchanged || len(records) == 0 {
				return keepIf(sl)(unchanged, err)
			}
			return &logspb.ScopeLogs{Scope: sl.Scope, SchemaUrl: sl.SchemaUrl, LogRecords: records}, nil
		})
		if err != nil || unchanged || len(scopes) == 0 {
			return keepIf(rl)(unchanged, err)
		}
		return &logspb.ResourceLogs{Resource: rl.Resource, SchemaUrl: rl.SchemaUrl, ScopeLogs: scopes}, nil
	})
	if err != nil || unchanged || len(resources) == 0 {
		return keepIf(data)(unchanged, err)
	}
	return &logspb.LogsData{ResourceLogs: resources}, nil
}

func FilterSpans(data *tracepb.TracesData, keep SpanFilter) (*tracepb.TracesData, error) {
	if data == nil {
		return nil, nil
	}
	resources, unchanged, err := prune(data.ResourceSpans, func(rs *tracepb.ResourceSpans) (*tracepb.ResourceSpans, error) {
		scopes, unchanged, err := prune(rs.ScopeSpans, func(ss *tracepb.ScopeSpans) (*tracepb.ScopeSpans, error) {
			spans, unchanged, err := prune(ss.Spans, func(span *tracepb.Span) (*tracepb.Span, error) {
				return keepIf(span)(keep(rs.Resource, ss.Scope, span))
			})
			if err != nil || unchanged || len(spans) == 0 {
				return keepIf(ss)(unchanged, err)
			}
			return &tracepb.ScopeSpans{Scope: ss.Scope, SchemaUrl: ss.SchemaUrl, Spans: spans}, nil
		})
		if err != nil || unchanged || len(scopes) == 0 {
			return keepIf(rs)(unchanged, err)
		}
		return &tracepb.ResourceSpans{Resource: rs.Resource, SchemaUrl: rs.SchemaUrl, ScopeSpans: scopes}, nil
	})
	if err != nil || unchanged || len(resources) == 0 {
		return keepIf(data)(unchanged, err)
	}
	return &tracepb.TracesData{ResourceSpans: resources}, nil
}

func FilterMetrics(data *metricspb.MetricsData, keep MetricFilter) (*metricspb.MetricsData, error) {
	if data == nil {
		return nil, nil
	}
	resources, unchanged, err := prune(data.ResourceMetrics, func(rm *metricspb.ResourceMetrics) (*metricspb.ResourceMetrics, error) {
		scopes, unchanged, err := prune(rm.ScopeMetrics, func(sm *metricspb.ScopeMetrics) (*metricspb.ScopeMetrics, error) {
			metrics, unchanged, err := prune(sm.Metrics, func(m *metricspb.Metric) (*metricspb.Metric, error) {
				return filterDataPoints(m, func(dp proto.Message) (bool, error) {
					return keep(rm.Resource, sm.Scope, m, dp)
				})
			})
			if err != nil || unchanged || len(metrics) == 0 {
				return keepIf(sm)(unchanged, err)
			}
			return &metricspb.ScopeMetrics{Scope: sm.Scope, SchemaUrl: sm.SchemaUrl, Metrics: metrics}, nil
		})
		if err != nil || unchanged || len(scopes) == 0 {
			return keepIf(rm)(unchanged, err)
		}
		return &metricspb.ResourceMetrics{Resource: rm.Resource, SchemaUrl: rm.SchemaUrl, ScopeMetrics: scopes}, nil
	})
	if err != nil || unchanged || len(resources) == 0 {
		return keepIf(data)(unchanged, err)
	}
	return &metricspb.MetricsData{ResourceMetrics: resources}, nil
}

// filterDataPoints returns the metric with only the data points keep accepts,
// or nil if none is kept. Metrics without data points are kept unchanged.
func filterDataPoints(m *metricspb.Metric, keep func(proto.Message) (bool, error)) (*metricspb.Metric, error) {
	switch data := m.Data.(type) {
	case *metricspb.Metric_Gauge:
		dps, unchanged, err := pruneDataPoints(data.Gauge.DataPoints, keep)
		if err != nil || unchanged || len(dps) == 0 {
			return keepIf(m)(unchanged, err)
		}
		out := withoutData(m)
		out.Data = &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{DataPoints: dps}}
		return out, nil
	case *metricspb.Metric_Sum:
		dps, unchanged, err := pruneDataPoints(data.Sum.DataPoints, keep)
		if err != nil || unchanged || len(dps) == 0 {
			return keepIf(m)(unchanged, err)
		}
		out := withoutData(m)
		out.Data = &metricspb.Metric_Sum{Sum: &metricspb.Sum{
			DataPoints:             dps,
			AggregationTemporality: data.Sum.AggregationTemporality,
			IsMonotonic:            data.Sum.IsMonotonic,
		}}
		return out, nil
	case *metricspb.Metric_Histogram:
		dps, unchanged, err := pruneDataPoints(data.Histogram.DataPoints, keep)
		if err != nil || unchanged || len(dps) == 0 {
			return keepIf(m)(unchanged, err)
		}
		out := withoutData(m)
		out.Data = &metricspb.Metric_Histogram{Histogram: &metricspb.Histogram{
			DataPoints:             dps,
			AggregationTemporality: data.Histogram.AggregationTemporality,
		}}
		return out, nil
	case *metricspb.Metric_ExponentialHistogram:
		dps, unchanged, err := pruneDataPoints(data.ExponentialHistogram.DataPoints, keep)
		if err != nil || unchanged || len(dps) == 0 {
			return keepIf(m)(unchanged, err)
		}
		out := withoutData(m)
		out.Data = &metricspb.Metric_ExponentialHistogram{ExponentialHistogram: &metricspb.ExponentialHistogram{
			DataPoints:             dps,
			AggregationTemporality: data.ExponentialHistogram.AggregationTemporality,
		}}
		return out, nil
	case *metricspb.Metric_Summary:
		dps, unchanged, err := pruneDataPoints(data.Summary.DataPoints, keep)
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
	return &metricspb.Metric{Name: m.Name, Description: m.Description, Unit: m.Unit, Metadata: m.Metadata}
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
func prune[T comparable](in []T, f func(T) (T, error)) (out []T, unchanged bool, err error) {
	var zero T
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
