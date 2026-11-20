// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

// Package otlpfuzz builds OTLP payloads from fuzz data and checks that a
// payload was partitioned correctly by the key attribute of its entries.
package otlpfuzz

import (
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"codeberg.org/tram/tram/internal/core/domain"
)

// KeyAttribute is the attribute of every log record, span and data point that
// holds its key.
const KeyAttribute = "key"

const (
	maxResources = 3
	maxScopes    = 2
	maxEntries   = 4
	maxPoints    = 3
	// One in nilOneIn resources, scopes and signals is left out.
	nilOneIn = 5
	// One in emptyMetricOneIn metrics has no data points.
	emptyMetricOneIn = 16

	resourceSchema = "resource-schema"
	scopeSchema    = "scope-schema"
)

const (
	gauge = iota
	sum
	histogram
	exponentialHistogram
	summary
	metricTypes
)

// Seeds returns fuzz data for a seed corpus: random bytes from a fixed source,
// so they are the same on every run.
func Seeds() [][]byte {
	source := rand.NewChaCha8([32]byte{})
	seeds := [][]byte{nil}
	for _, size := range []int{16, 64, 128, 256, 256, 512, 512, 1024} {
		seed := make([]byte, size)
		_, _ = source.Read(seed)
		seeds = append(seeds, seed)
	}
	return seeds
}

// Input is a payload built from fuzz data, with what is known about each of
// its entries. Entries are identified by an ID in their timestamp, which
// increases in the order of the payload.
type Input struct {
	Event *domain.Event
	// MetricWithoutDataPoints is set if a metric has no data points.
	MetricWithoutDataPoints bool

	before  *domain.Event
	entries map[uint64]entry
}

type entry struct {
	signal   domain.Signal
	key      string
	resource *resourcepb.Resource
	scope    *commonpb.InstrumentationScope
	metric   *metricspb.Metric
}

// Part is a payload that a partitioning produced for a key.
type Part struct {
	Key   string
	Event *domain.Event
}

type reader struct {
	data []byte
	next int
}

// byteMod returns the next byte of the fuzz data modulo n, or 0 once the data
// is used up.
func (r *reader) byteMod(n int) int {
	if r.next >= len(r.data) {
		return 0
	}
	b := r.data[r.next]
	r.next++
	return int(b) % n
}

// leftOut decides whether to leave out a signal, resource or scope; once the
// data is used up, all are left out.
func (r *reader) leftOut() bool { return r.byteMod(nilOneIn) == 0 }

// pick returns one of options, chosen by the fuzz data.
func pick[T any](r *reader, options ...T) T {
	return options[r.byteMod(len(options))]
}

// Build builds a payload of logs, metrics and traces from data.
func Build(data []byte) *Input {
	b := &builder{r: &reader{data: data}, in: &Input{entries: map[uint64]entry{}}}
	b.in.Event = &domain.Event{Logs: b.logs(), Metrics: b.metrics(), Traces: b.traces()}
	b.in.before = &domain.Event{
		Logs:    clone(b.in.Event.Logs),
		Metrics: clone(b.in.Event.Metrics),
		Traces:  clone(b.in.Event.Traces),
	}
	return b.in
}

func clone[T proto.Message](m T) T {
	out, _ := proto.Clone(m).(T)
	return out
}

type builder struct {
	r      *reader
	in     *Input
	lastID uint64
}

func (b *builder) key() string {
	return pick(b.r, "a", "b", "c")
}

func (b *builder) id() uint64 {
	b.lastID++
	return b.lastID
}

func keyAttributes(key string) []*commonpb.KeyValue {
	return []*commonpb.KeyValue{
		{Key: "other", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "x"}}},
		{Key: KeyAttribute, Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: key}}},
	}
}

func (b *builder) resource(i int) *resourcepb.Resource {
	if b.r.leftOut() {
		return nil
	}
	return &resourcepb.Resource{Attributes: []*commonpb.KeyValue{{
		Key:   "service.name",
		Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: fmt.Sprintf("service-%d", i)}},
	}}}
}

func (b *builder) scope(i int) *commonpb.InstrumentationScope {
	if b.r.leftOut() {
		return nil
	}
	return &commonpb.InstrumentationScope{Name: fmt.Sprintf("scope-%d", i)}
}

//nolint:dupl // Same shape as traces, for another signal.
func (b *builder) logs() *logspb.LogsData {
	if b.r.leftOut() {
		return nil
	}
	data := &logspb.LogsData{}
	for i := range 1 + b.r.byteMod(maxResources) {
		rl := &logspb.ResourceLogs{Resource: b.resource(i), SchemaUrl: resourceSchema}
		for j := range 1 + b.r.byteMod(maxScopes) {
			sl := &logspb.ScopeLogs{Scope: b.scope(j), SchemaUrl: scopeSchema}
			for range b.r.byteMod(maxEntries + 1) {
				id, key := b.id(), b.key()
				sl.LogRecords = append(
					sl.LogRecords,
					&logspb.LogRecord{TimeUnixNano: id, Attributes: keyAttributes(key)},
				)
				b.in.entries[id] = entry{
					signal:   domain.SignalLogs,
					key:      key,
					resource: rl.GetResource(),
					scope:    sl.GetScope(),
				}
			}
			rl.ScopeLogs = append(rl.ScopeLogs, sl)
		}
		data.ResourceLogs = append(data.ResourceLogs, rl)
	}
	return data
}

//nolint:dupl // Same shape as logs, for another signal.
func (b *builder) traces() *tracepb.TracesData {
	if b.r.leftOut() {
		return nil
	}
	data := &tracepb.TracesData{}
	for i := range 1 + b.r.byteMod(maxResources) {
		rs := &tracepb.ResourceSpans{Resource: b.resource(i), SchemaUrl: resourceSchema}
		for j := range 1 + b.r.byteMod(maxScopes) {
			ss := &tracepb.ScopeSpans{Scope: b.scope(j), SchemaUrl: scopeSchema}
			for range b.r.byteMod(maxEntries + 1) {
				id, key := b.id(), b.key()
				ss.Spans = append(ss.Spans, &tracepb.Span{StartTimeUnixNano: id, Attributes: keyAttributes(key)})
				b.in.entries[id] = entry{
					signal:   domain.SignalTraces,
					key:      key,
					resource: rs.GetResource(),
					scope:    ss.GetScope(),
				}
			}
			rs.ScopeSpans = append(rs.ScopeSpans, ss)
		}
		data.ResourceSpans = append(data.ResourceSpans, rs)
	}
	return data
}

func (b *builder) metrics() *metricspb.MetricsData {
	if b.r.leftOut() {
		return nil
	}
	data := &metricspb.MetricsData{}
	for i := range 1 + b.r.byteMod(maxResources) {
		rm := &metricspb.ResourceMetrics{Resource: b.resource(i), SchemaUrl: resourceSchema}
		for j := range 1 + b.r.byteMod(maxScopes) {
			sm := &metricspb.ScopeMetrics{Scope: b.scope(j), SchemaUrl: scopeSchema}
			for range b.r.byteMod(maxEntries + 1) {
				m := b.metric(len(sm.GetMetrics()))
				sm.Metrics = append(sm.Metrics, m)
				for _, id := range dataPointIDs(m) {
					e := b.in.entries[id]
					e.metric, e.resource, e.scope = m, rm.GetResource(), sm.GetScope()
					b.in.entries[id] = e
				}
			}
			rm.ScopeMetrics = append(rm.ScopeMetrics, sm)
		}
		data.ResourceMetrics = append(data.ResourceMetrics, rm)
	}
	return data
}

// metric builds a metric of a type chosen by the fuzz data, with 0 to 3 data
// points and every other field of its type set.
func (b *builder) metric(i int) *metricspb.Metric {
	m := &metricspb.Metric{
		Name:        fmt.Sprintf("metric-%d", i),
		Description: "described",
		Unit:        "1",
		Metadata:    keyAttributes("meta"),
	}
	// A metric without data points fails a subject that reads datapoint, and
	// with it the whole payload, so it is rare enough for most payloads to be
	// partitioned. It is not chosen by 0, which data that is used up returns.
	count := 1 + b.r.byteMod(maxPoints)
	if b.r.byteMod(emptyMetricOneIn) == emptyMetricOneIn-1 {
		count = 0
		b.in.MetricWithoutDataPoints = true
	}
	temporality := pick(b.r,
		metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA,
		metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE)
	switch b.r.byteMod(metricTypes) {
	case gauge:
		m.Data = &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{DataPoints: numberPoints(b, count)}}
	case sum:
		m.Data = &metricspb.Metric_Sum{Sum: &metricspb.Sum{
			DataPoints: numberPoints(
				b,
				count,
			),
			AggregationTemporality: temporality,
			IsMonotonic:            pick(b.r, false, true),
		}}
	case histogram:
		points := make([]*metricspb.HistogramDataPoint, count)
		for k := range points {
			id, key := b.point()
			points[k] = &metricspb.HistogramDataPoint{TimeUnixNano: id, Attributes: keyAttributes(key)}
		}
		m.Data = &metricspb.Metric_Histogram{Histogram: &metricspb.Histogram{
			DataPoints: points, AggregationTemporality: temporality,
		}}
	case exponentialHistogram:
		points := make([]*metricspb.ExponentialHistogramDataPoint, count)
		for k := range points {
			id, key := b.point()
			points[k] = &metricspb.ExponentialHistogramDataPoint{TimeUnixNano: id, Attributes: keyAttributes(key)}
		}
		m.Data = &metricspb.Metric_ExponentialHistogram{ExponentialHistogram: &metricspb.ExponentialHistogram{
			DataPoints: points, AggregationTemporality: temporality,
		}}
	case summary:
		points := make([]*metricspb.SummaryDataPoint, count)
		for k := range points {
			id, key := b.point()
			points[k] = &metricspb.SummaryDataPoint{TimeUnixNano: id, Attributes: keyAttributes(key)}
		}
		m.Data = &metricspb.Metric_Summary{Summary: &metricspb.Summary{DataPoints: points}}
	}
	return m
}

func numberPoints(b *builder, count int) []*metricspb.NumberDataPoint {
	points := make([]*metricspb.NumberDataPoint, count)
	for k := range points {
		id, key := b.point()
		points[k] = &metricspb.NumberDataPoint{TimeUnixNano: id, Attributes: keyAttributes(key)}
	}
	return points
}

// point registers a data point; its metric is added once the metric is built.
func (b *builder) point() (uint64, string) {
	id, key := b.id(), b.key()
	b.in.entries[id] = entry{signal: domain.SignalMetrics, key: key}
	return id, key
}

// Check checks that parts partition the input by key: every entry is in
// exactly one part, the part of its key, with its resource, scope and, for
// data points, the fields of its metric; entries keep their order; there is
// one part per signal and key; and the input is unchanged.
func Check(t *testing.T, in *Input, parts []Part) {
	t.Helper()

	seen := map[uint64]bool{}
	type signalKey struct {
		signal domain.Signal
		key    string
	}
	partOf := map[signalKey]int{}
	for i, part := range parts {
		signal, ok := onlySignal(part.Event)
		require.True(t, ok, "part %d holds exactly one signal", i)
		require.NotContains(
			t,
			partOf,
			signalKey{signal, part.Key},
			"part %d: one part per signal and key, %s %q is also in part %d",
			i,
			signal,
			part.Key,
			partOf[signalKey{signal, part.Key}],
		)
		partOf[signalKey{signal, part.Key}] = i

		var last uint64
		visit(
			part.Event,
			func(id uint64, resource *resourcepb.Resource, scope *commonpb.InstrumentationScope, m *metricspb.Metric) {
				want, known := in.entries[id]
				require.True(t, known, "part %d holds an entry %d that is not in the input", i, id)
				require.False(t, seen[id], "entry %d is in more than one part", id)
				seen[id] = true
				assert.Equal(t, want.key, part.Key, "entry %d is in the part of its key", id)
				assert.Equal(t, want.signal, signal, "entry %d keeps its signal", id)
				assert.Same(t, want.resource, resource, "entry %d keeps its resource", id)
				assert.Same(t, want.scope, scope, "entry %d keeps its scope", id)
				if m != nil {
					assert.True(t, proto.Equal(withoutDataPoints(want.metric), withoutDataPoints(m)),
						"data point %d keeps the fields of its metric", id)
				}
				assert.Greater(t, id, last, "entries keep their order in part %d", i)
				last = id
			},
		)
	}
	for id := range in.entries {
		assert.True(t, seen[id], "entry %d is in a part", id)
	}

	assert.True(t, proto.Equal(in.before.Logs, in.Event.Logs), "the input logs are unchanged")
	assert.True(t, proto.Equal(in.before.Metrics, in.Event.Metrics), "the input metrics are unchanged")
	assert.True(t, proto.Equal(in.before.Traces, in.Event.Traces), "the input traces are unchanged")
}

func onlySignal(e *domain.Event) (domain.Signal, bool) {
	var signals []domain.Signal
	if e.Logs != nil {
		signals = append(signals, domain.SignalLogs)
	}
	if e.Metrics != nil {
		signals = append(signals, domain.SignalMetrics)
	}
	if e.Traces != nil {
		signals = append(signals, domain.SignalTraces)
	}
	if len(signals) != 1 {
		return "", false
	}
	return signals[0], true
}

// visit calls f for every entry of e with its ID, resource and scope, and for
// data points their metric.
func visit(e *domain.Event, f visitor) {
	visitLogs(e.Logs, f)
	visitTraces(e.Traces, f)
	visitMetrics(e.Metrics, f)
}

type visitor func(id uint64, resource *resourcepb.Resource, scope *commonpb.InstrumentationScope, m *metricspb.Metric)

func visitLogs(data *logspb.LogsData, f visitor) {
	for _, rl := range data.GetResourceLogs() {
		for _, sl := range rl.GetScopeLogs() {
			for _, record := range sl.GetLogRecords() {
				f(record.GetTimeUnixNano(), rl.GetResource(), sl.GetScope(), nil)
			}
		}
	}
}

func visitTraces(data *tracepb.TracesData, f visitor) {
	for _, rs := range data.GetResourceSpans() {
		for _, ss := range rs.GetScopeSpans() {
			for _, span := range ss.GetSpans() {
				f(span.GetStartTimeUnixNano(), rs.GetResource(), ss.GetScope(), nil)
			}
		}
	}
}

func visitMetrics(data *metricspb.MetricsData, f visitor) {
	for _, rm := range data.GetResourceMetrics() {
		for _, sm := range rm.GetScopeMetrics() {
			for _, m := range sm.GetMetrics() {
				for _, id := range dataPointIDs(m) {
					f(id, rm.GetResource(), sm.GetScope(), m)
				}
			}
		}
	}
}

// dataPointIDs returns the IDs of the data points of m, of any type.
func dataPointIDs(m *metricspb.Metric) []uint64 {
	var ids []uint64
	m.ProtoReflect().Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		if field.ContainingOneof() == nil {
			return true
		}
		inner := value.Message()
		list := inner.Get(inner.Descriptor().Fields().ByName("data_points")).List()
		for i := range list.Len() {
			point := list.Get(i).Message()
			ids = append(ids, point.Get(point.Descriptor().Fields().ByName("time_unix_nano")).Uint())
		}
		return true
	})
	return ids
}

// withoutDataPoints is a copy of m without its data points, to compare the
// other fields.
func withoutDataPoints(m *metricspb.Metric) *metricspb.Metric {
	out := clone(m)
	out.ProtoReflect().Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		if field.ContainingOneof() != nil {
			inner := value.Message()
			inner.Clear(inner.Descriptor().Fields().ByName("data_points"))
		}
		return true
	})
	return out
}
