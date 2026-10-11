// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package domain

import (
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

// Part is the part of a payload whose entries share a key.
type Part[D any] struct {
	Key  string
	Data D
}

// LogKey returns the key of a log record in its resource and scope.
type LogKey func(*logspb.ResourceLogs, *logspb.ScopeLogs, *logspb.LogRecord) (string, error)

// SpanKey returns the key of a span in its resource and scope.
type SpanKey func(*tracepb.ResourceSpans, *tracepb.ScopeSpans, *tracepb.Span) (string, error)

// MetricKey returns the key of a data point of a metric in its resource and
// scope. dataPoint is nil for a metric without data points, which is then
// keyed as a whole.
type MetricKey func(
	*metricspb.ResourceMetrics, *metricspb.ScopeMetrics, *metricspb.Metric, proto.Message,
) (string, error)

// PartitionLogs splits data into one payload per key of its log records, in
// the order the keys first appear. A part holds its records in their original
// order, in copies of their resource and scope containers, which share the
// resource and scope. If all records share a key, data itself is the only
// part; without records there is none.
func PartitionLogs(data *logspb.LogsData, key LogKey) ([]Part[*logspb.LogsData], error) {
	return partition(logsShape(), data, key)
}

// PartitionSpans splits data into one payload per key of its spans, like
// PartitionLogs.
func PartitionSpans(data *tracepb.TracesData, key SpanKey) ([]Part[*tracepb.TracesData], error) {
	return partition(tracesShape(), data, key)
}

// PartitionMetrics splits data into one payload per key of its data points,
// like PartitionLogs. A metric whose data points have different keys is split
// first, see AppendMetricParts.
func PartitionMetrics(data *metricspb.MetricsData, key MetricKey) ([]Part[*metricspb.MetricsData], error) {
	var (
		rm *metricspb.ResourceMetrics
		sm *metricspb.ScopeMetrics
		m  *metricspb.Metric
	)
	dataPointKey := func(dataPoint proto.Message) (string, error) {
		return key(rm, sm, m, dataPoint)
	}

	var parts []MetricPart
	var counts []int // parts per metric
	for _, rm = range data.GetResourceMetrics() {
		for _, sm = range rm.GetScopeMetrics() {
			for _, m = range sm.GetMetrics() {
				before := len(parts)
				var err error
				if parts, err = AppendMetricParts(parts, m, dataPointKey); err != nil {
					return nil, err
				}
				counts = append(counts, len(parts)-before)
			}
		}
	}

	keys := make([]string, len(parts))
	for i, part := range parts {
		keys[i] = part.Key
	}
	split := data
	if len(parts) != len(counts) {
		// A metric was split into several parts.
		split = withMetricParts(data, parts, counts)
	}
	return byKeys(metricsShape(), split, keys), nil
}

// withMetricParts copies the structure of data, with the metrics replaced by
// their parts. counts holds the number of parts of each metric.
func withMetricParts(data *metricspb.MetricsData, parts []MetricPart, counts []int) *metricspb.MetricsData {
	sh := metricsShape()
	out := sh.newPayload()
	metric, part := 0, 0
	for _, rm := range data.GetResourceMetrics() {
		resource := sh.newResource(rm)
		sh.appendResource(out, resource)
		for _, sm := range rm.GetScopeMetrics() {
			scope := sh.newScope(sm)
			sh.appendScope(resource, scope)
			for range sm.GetMetrics() {
				for range counts[metric] {
					sh.appendEntry(scope, parts[part].Metric)
					part++
				}
				metric++
			}
		}
	}
	return out
}

func partition[D any, R, S, E comparable](
	sh shape[D, R, S, E],
	data D,
	key func(R, S, E) (string, error),
) ([]Part[D], error) {
	var keys []string
	for _, r := range sh.resources(data) {
		for _, s := range sh.scopes(r) {
			for _, e := range sh.entries(s) {
				k, err := key(r, s, e)
				if err != nil {
					return nil, err
				}
				keys = append(keys, k)
			}
		}
	}
	return byKeys(sh, data, keys), nil
}

// byKeys groups the entries of data, in order, by keys, which holds one key
// per entry.
func byKeys[D any, R, S, E comparable](sh shape[D, R, S, E], data D, keys []string) []Part[D] {
	if len(keys) == 0 {
		return nil
	}
	if allEqual(keys) {
		return []Part[D]{{Key: keys[0], Data: data}}
	}

	var order []string
	groups := map[string]*group[D, R, S, E]{}
	i := 0
	for _, r := range sh.resources(data) {
		for _, s := range sh.scopes(r) {
			for _, e := range sh.entries(s) {
				g := groups[keys[i]]
				if g == nil {
					g = &group[D, R, S, E]{shape: sh, data: sh.newPayload()}
					groups[keys[i]] = g
					order = append(order, keys[i])
				}
				g.add(r, s, e)
				i++
			}
		}
	}

	parts := make([]Part[D], len(order))
	for j, k := range order {
		parts[j] = Part[D]{Key: k, Data: groups[k].data}
	}
	return parts
}

func allEqual(keys []string) bool {
	for _, k := range keys[1:] {
		if k != keys[0] {
			return false
		}
	}
	return true
}

// shape describes the levels of an OTLP payload: resources, scopes and
// entries, and how to build copies of its containers.
type shape[D any, R, S, E comparable] struct {
	resources      func(D) []R
	scopes         func(R) []S
	entries        func(S) []E
	newPayload     func() D
	newResource    func(R) R
	newScope       func(S) S
	appendResource func(D, R)
	appendScope    func(R, S)
	appendEntry    func(S, E)
}

// group collects the entries of one key into a payload of its own. Entries
// that follow each other in the same resource and scope share the copies of
// their containers.
type group[D any, R, S, E comparable] struct {
	shape[D, R, S, E]

	data                D
	resourceIn, scopeIn any
	resource            R
	scope               S
	started             bool
}

func (g *group[D, R, S, E]) add(r R, s S, e E) {
	if !g.started || g.resourceIn != any(r) {
		g.resource = g.newResource(r)
		g.appendResource(g.data, g.resource)
		g.resourceIn, g.scopeIn = r, nil
		g.started = true
	}
	if g.scopeIn != any(s) {
		g.scope = g.newScope(s)
		g.appendScope(g.resource, g.scope)
		g.scopeIn = s
	}
	g.appendEntry(g.scope, e)
}

//nolint:dupl // Same shape for each OTLP type; the shared logic is in byKeys.
func logsShape() shape[*logspb.LogsData, *logspb.ResourceLogs, *logspb.ScopeLogs, *logspb.LogRecord] {
	return shape[*logspb.LogsData, *logspb.ResourceLogs, *logspb.ScopeLogs, *logspb.LogRecord]{
		resources:  (*logspb.LogsData).GetResourceLogs,
		scopes:     (*logspb.ResourceLogs).GetScopeLogs,
		entries:    (*logspb.ScopeLogs).GetLogRecords,
		newPayload: func() *logspb.LogsData { return &logspb.LogsData{} },
		newResource: func(r *logspb.ResourceLogs) *logspb.ResourceLogs {
			return &logspb.ResourceLogs{Resource: r.GetResource(), SchemaUrl: r.GetSchemaUrl()}
		},
		newScope: func(s *logspb.ScopeLogs) *logspb.ScopeLogs {
			return &logspb.ScopeLogs{Scope: s.GetScope(), SchemaUrl: s.GetSchemaUrl()}
		},
		appendResource: func(d *logspb.LogsData, r *logspb.ResourceLogs) { d.ResourceLogs = append(d.ResourceLogs, r) },
		appendScope:    func(r *logspb.ResourceLogs, s *logspb.ScopeLogs) { r.ScopeLogs = append(r.ScopeLogs, s) },
		appendEntry:    func(s *logspb.ScopeLogs, e *logspb.LogRecord) { s.LogRecords = append(s.LogRecords, e) },
	}
}

//nolint:dupl // Same shape for each OTLP type; the shared logic is in byKeys.
func metricsShape() shape[*metricspb.MetricsData, *metricspb.ResourceMetrics, *metricspb.ScopeMetrics, *metricspb.Metric] {
	return shape[*metricspb.MetricsData, *metricspb.ResourceMetrics, *metricspb.ScopeMetrics, *metricspb.Metric]{
		resources:  (*metricspb.MetricsData).GetResourceMetrics,
		scopes:     (*metricspb.ResourceMetrics).GetScopeMetrics,
		entries:    (*metricspb.ScopeMetrics).GetMetrics,
		newPayload: func() *metricspb.MetricsData { return &metricspb.MetricsData{} },
		newResource: func(r *metricspb.ResourceMetrics) *metricspb.ResourceMetrics {
			return &metricspb.ResourceMetrics{Resource: r.GetResource(), SchemaUrl: r.GetSchemaUrl()}
		},
		newScope: func(s *metricspb.ScopeMetrics) *metricspb.ScopeMetrics {
			return &metricspb.ScopeMetrics{Scope: s.GetScope(), SchemaUrl: s.GetSchemaUrl()}
		},
		appendResource: func(d *metricspb.MetricsData, r *metricspb.ResourceMetrics) {
			d.ResourceMetrics = append(d.ResourceMetrics, r)
		},
		appendScope: func(r *metricspb.ResourceMetrics, s *metricspb.ScopeMetrics) {
			r.ScopeMetrics = append(r.ScopeMetrics, s)
		},
		appendEntry: func(s *metricspb.ScopeMetrics, e *metricspb.Metric) { s.Metrics = append(s.Metrics, e) },
	}
}

//nolint:dupl // Same shape for each OTLP type; the shared logic is in byKeys.
func tracesShape() shape[*tracepb.TracesData, *tracepb.ResourceSpans, *tracepb.ScopeSpans, *tracepb.Span] {
	return shape[*tracepb.TracesData, *tracepb.ResourceSpans, *tracepb.ScopeSpans, *tracepb.Span]{
		resources:  (*tracepb.TracesData).GetResourceSpans,
		scopes:     (*tracepb.ResourceSpans).GetScopeSpans,
		entries:    (*tracepb.ScopeSpans).GetSpans,
		newPayload: func() *tracepb.TracesData { return &tracepb.TracesData{} },
		newResource: func(r *tracepb.ResourceSpans) *tracepb.ResourceSpans {
			return &tracepb.ResourceSpans{Resource: r.GetResource(), SchemaUrl: r.GetSchemaUrl()}
		},
		newScope: func(s *tracepb.ScopeSpans) *tracepb.ScopeSpans {
			return &tracepb.ScopeSpans{Scope: s.GetScope(), SchemaUrl: s.GetSchemaUrl()}
		},
		appendResource: func(d *tracepb.TracesData, r *tracepb.ResourceSpans) {
			d.ResourceSpans = append(d.ResourceSpans, r)
		},
		appendScope: func(r *tracepb.ResourceSpans, s *tracepb.ScopeSpans) { r.ScopeSpans = append(r.ScopeSpans, s) },
		appendEntry: func(s *tracepb.ScopeSpans, e *tracepb.Span) { s.Spans = append(s.Spans, e) },
	}
}
