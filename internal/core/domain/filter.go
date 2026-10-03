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

//nolint:dupl // Same shape for another OTLP type; the shared logic is in filterTree.
func FilterLogs(data *logspb.LogsData, keep LogFilter) (*logspb.LogsData, error) {
	if data == nil {
		return data, nil
	}
	resources, unchanged, err := filterTree(
		data.GetResourceLogs(),
		(*logspb.ResourceLogs).GetScopeLogs,
		(*logspb.ScopeLogs).GetLogRecords,
		func(rl *logspb.ResourceLogs, sl *logspb.ScopeLogs, record *logspb.LogRecord) (*logspb.LogRecord, error) {
			return keepIf(record)(keep(rl.GetResource(), sl.GetScope(), record))
		},
		func(sl *logspb.ScopeLogs, records []*logspb.LogRecord) *logspb.ScopeLogs {
			return &logspb.ScopeLogs{Scope: sl.GetScope(), SchemaUrl: sl.GetSchemaUrl(), LogRecords: records}
		},
		func(rl *logspb.ResourceLogs, scopes []*logspb.ScopeLogs) *logspb.ResourceLogs {
			return &logspb.ResourceLogs{Resource: rl.GetResource(), SchemaUrl: rl.GetSchemaUrl(), ScopeLogs: scopes}
		},
	)
	if err != nil || unchanged || len(resources) == 0 {
		return keepIf(data)(unchanged, err)
	}
	return &logspb.LogsData{ResourceLogs: resources}, nil
}

//nolint:dupl // Same shape for another OTLP type; the shared logic is in filterTree.
func FilterSpans(data *tracepb.TracesData, keep SpanFilter) (*tracepb.TracesData, error) {
	if data == nil {
		return data, nil
	}
	resources, unchanged, err := filterTree(
		data.GetResourceSpans(),
		(*tracepb.ResourceSpans).GetScopeSpans,
		(*tracepb.ScopeSpans).GetSpans,
		func(rs *tracepb.ResourceSpans, ss *tracepb.ScopeSpans, span *tracepb.Span) (*tracepb.Span, error) {
			return keepIf(span)(keep(rs.GetResource(), ss.GetScope(), span))
		},
		func(ss *tracepb.ScopeSpans, spans []*tracepb.Span) *tracepb.ScopeSpans {
			return &tracepb.ScopeSpans{Scope: ss.GetScope(), SchemaUrl: ss.GetSchemaUrl(), Spans: spans}
		},
		func(rs *tracepb.ResourceSpans, scopes []*tracepb.ScopeSpans) *tracepb.ResourceSpans {
			return &tracepb.ResourceSpans{Resource: rs.GetResource(), SchemaUrl: rs.GetSchemaUrl(), ScopeSpans: scopes}
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
	resources, unchanged, err := filterTree(
		data.GetResourceMetrics(),
		(*metricspb.ResourceMetrics).GetScopeMetrics,
		(*metricspb.ScopeMetrics).GetMetrics,
		func(rm *metricspb.ResourceMetrics, sm *metricspb.ScopeMetrics, m *metricspb.Metric) (*metricspb.Metric, error) {
			return filterDataPoints(m, func(dp proto.Message) (bool, error) {
				return keep(rm.GetResource(), sm.GetScope(), m, dp)
			})
		},
		func(sm *metricspb.ScopeMetrics, metrics []*metricspb.Metric) *metricspb.ScopeMetrics {
			return &metricspb.ScopeMetrics{Scope: sm.GetScope(), SchemaUrl: sm.GetSchemaUrl(), Metrics: metrics}
		},
		func(rm *metricspb.ResourceMetrics, scopes []*metricspb.ScopeMetrics) *metricspb.ResourceMetrics {
			return &metricspb.ResourceMetrics{
				Resource:     rm.GetResource(),
				SchemaUrl:    rm.GetSchemaUrl(),
				ScopeMetrics: scopes,
			}
		},
	)
	if err != nil || unchanged || len(resources) == 0 {
		return keepIf(data)(unchanged, err)
	}
	return &metricspb.MetricsData{ResourceMetrics: resources}, nil
}

// filterTree filters the resource -> scope -> entry structure all signals
// share. entry returns the entry to keep (the original or a reduced copy), or
// the zero value to drop it. newScope and newResource rebuild a container
// with only the kept children; unchanged containers are reused.
func filterTree[R, S, E comparable](
	resources []R,
	scopesOf func(R) []S,
	entriesOf func(S) []E,
	entry func(R, S, E) (E, error),
	newScope func(S, []E) S,
	newResource func(R, []S) R,
) ([]R, bool, error) {
	return prune(resources, func(resource R) (R, error) {
		scopes, scopesUnchanged, err := prune(scopesOf(resource), func(scope S) (S, error) {
			entries, entriesUnchanged, err := prune(entriesOf(scope), func(e E) (E, error) {
				return entry(resource, scope, e)
			})
			if err != nil || entriesUnchanged || len(entries) == 0 {
				return keepIf(scope)(entriesUnchanged, err)
			}
			return newScope(scope, entries), nil
		})
		if err != nil || scopesUnchanged || len(scopes) == 0 {
			return keepIf(resource)(scopesUnchanged, err)
		}
		return newResource(resource, scopes), nil
	})
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
