// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package valuemap_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-playground/validator/v10"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"

	"log/slog"

	"codeberg.org/tram/tram/internal/adapters/stage/valuemap"
	"codeberg.org/tram/tram/internal/core/domain"
	"codeberg.org/tram/tram/internal/testutil/otlpgen"
)

const (
	team0     = "team-0"
	team1     = "team-1"
	teamIDKey = "team.id"
)

type capture struct{ events []*domain.Event }

func (c *capture) Name() string { return "capture" }
func (c *capture) From() string { return "value_map" }
func (c *capture) Stop() error  { return nil }
func (c *capture) Notify(_ context.Context, e *domain.Event) error {
	c.events = append(c.events, e)
	return nil
}

func newStage(t *testing.T, spec map[string]any) (*valuemap.ValueMap, *capture) {
	t.Helper()
	logger := slog.Default()
	s, err := valuemap.New(domain.StageSpec{Name: "mapper", From: "source", Spec: spec}, validator.New(), logger)
	require.NoError(t, err)
	c := &capture{}
	require.NoError(t, s.Subscribe(c))
	return s, c
}

func mappedSpec(table map[string]string) map[string]any {
	return map[string]any{
		"source_attr": "team",
		"target_attr": teamIDKey,
		"default":     "unknown",
		"table":       table,
	}
}

func stringAttr(key, value string) *commonpb.KeyValue {
	return &commonpb.KeyValue{
		Key:   key,
		Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: value}},
	}
}

func intAttr(key string, value int64) *commonpb.KeyValue {
	return &commonpb.KeyValue{
		Key:   key,
		Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: value}},
	}
}

func logs(resources ...[]*logspb.LogRecord) *domain.Event {
	var rls []*logspb.ResourceLogs
	for _, records := range resources {
		rls = append(rls, &logspb.ResourceLogs{
			Resource:  &resourcepb.Resource{Attributes: []*commonpb.KeyValue{stringAttr("service.name", "shop")}},
			ScopeLogs: []*logspb.ScopeLogs{{LogRecords: records}},
		})
	}
	return &domain.Event{Logs: &logspb.LogsData{ResourceLogs: rls}}
}

func record(attrs ...*commonpb.KeyValue) *logspb.LogRecord {
	return &logspb.LogRecord{Attributes: attrs}
}

// attr returns the string value of the first record's attribute with the key.
func attr(e *domain.Event, key string) (string, bool) {
	records := e.Logs.GetResourceLogs()[0].GetScopeLogs()[0].GetLogRecords()
	for _, kv := range records[0].GetAttributes() {
		if kv.GetKey() == key {
			return kv.GetValue().GetStringValue(), true
		}
	}
	return "", false
}

func TestNew(t *testing.T) {
	t.Parallel()

	tableFile := func(t *testing.T, content string) string {
		t.Helper()
		path := filepath.Join(t.TempDir(), "table.json")
		require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
		return path
	}

	cases := []struct {
		name        string
		spec        map[string]any
		expectErr   bool
		expectErrIs error
	}{{
		name: "embedded table",
		spec: mappedSpec(map[string]string{team0: "a"}),
	}, {
		name: "table from file",
		spec: map[string]any{
			"source_attr": "team", "target_attr": teamIDKey, "default": "unknown",
			"table_ref": tableFile(t, `{"team-0": "a"}`),
		},
	}, {
		name: "record level is the default",
		spec: func() map[string]any {
			spec := mappedSpec(map[string]string{team0: "a"})
			spec["level"] = "record"
			return spec
		}(),
	}, {
		name: "resource level",
		spec: func() map[string]any {
			spec := mappedSpec(map[string]string{team0: "a"})
			spec["level"] = "resource"
			return spec
		}(),
	}, {
		name:      "missing source_attr",
		spec:      map[string]any{"target_attr": teamIDKey, "default": "unknown", "table": map[string]any{}},
		expectErr: true,
	}, {
		name:      "missing target_attr",
		spec:      map[string]any{"source_attr": "team", "default": "unknown", "table": map[string]any{}},
		expectErr: true,
	}, {
		name:      "missing default",
		spec:      map[string]any{"source_attr": "team", "target_attr": teamIDKey, "table": map[string]any{}},
		expectErr: true,
	}, {
		name: "invalid level",
		spec: func() map[string]any {
			spec := mappedSpec(map[string]string{team0: "a"})
			spec["level"] = "scope"
			return spec
		}(),
		expectErr: true,
	}, {
		name:      "neither table nor table_ref",
		spec:      map[string]any{"source_attr": "team", "target_attr": teamIDKey, "default": "unknown"},
		expectErr: true,
	}, {
		name: "both table and table_ref",
		spec: map[string]any{
			"source_attr": "team", "target_attr": teamIDKey, "default": "unknown",
			"table": map[string]any{team0: "a"}, "table_ref": "table.json",
		},
		expectErr: true,
	}, {
		name: "table_ref file does not exist",
		spec: map[string]any{
			"source_attr": "team", "target_attr": teamIDKey, "default": "unknown",
			"table_ref": filepath.Join(t.TempDir(), "missing.json"),
		},
		expectErr:   true,
		expectErrIs: valuemap.ErrReadingTableRef,
	}, {
		name: "table_ref holds invalid JSON",
		spec: map[string]any{
			"source_attr": "team", "target_attr": teamIDKey, "default": "unknown",
			"table_ref": tableFile(t, `{"team-0": `),
		},
		expectErr:   true,
		expectErrIs: valuemap.ErrReadingTableRef,
	}, {
		name: "table_ref holds non-string values",
		spec: map[string]any{
			"source_attr": "team", "target_attr": teamIDKey, "default": "unknown",
			"table_ref": tableFile(t, `{"team-0": 7}`),
		},
		expectErr:   true,
		expectErrIs: valuemap.ErrReadingTableRef,
	}, {
		name:      "unknown field",
		spec:      map[string]any{"source_attr": "team", "table": map[string]any{}, "tables": true},
		expectErr: true,
	}}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			_, err := valuemap.New(domain.StageSpec{Name: "mapper", Spec: c.spec}, validator.New(), slog.Default())
			if !c.expectErr {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			if c.expectErrIs != nil {
				assert.ErrorIs(t, err, c.expectErrIs)
			}
		})
	}
}

func TestMappingHits(t *testing.T) {
	t.Parallel()

	s, out := newStage(t, mappedSpec(map[string]string{team0: "cart", team1: "checkout"}))

	event := logs(
		[]*logspb.LogRecord{
			record(stringAttr("team", team0)),
			record(stringAttr("team", team1), stringAttr("other", "kept")),
		},
	)

	require.NoError(t, s.Notify(t.Context(), event))
	require.Len(t, out.events, 1)

	mapped, ok := attr(out.events[0], teamIDKey)
	require.True(t, ok, "the target attribute is set")
	assert.Equal(t, "cart", mapped)

	// the second record of the same scope is mapped on its own
	second := out.events[0].Logs.GetResourceLogs()[0].GetScopeLogs()[0].GetLogRecords()[1]
	assert.Len(t, second.GetAttributes(), 3, "the source and other attributes are kept")
	values := map[string]string{}
	for _, kv := range second.GetAttributes() {
		values[kv.GetKey()] = kv.GetValue().GetStringValue()
	}
	assert.Equal(t, team1, values["team"])
	assert.Equal(t, "kept", values["other"])
	assert.Equal(t, "checkout", values[teamIDKey])
}

func TestLookupMissesSetDefault(t *testing.T) {
	t.Parallel()

	s, out := newStage(t, mappedSpec(map[string]string{team0: "cart"}))

	event := logs([]*logspb.LogRecord{
		record(stringAttr("team", team0)),    // mapped
		record(stringAttr("team", "team-9")), // no table entry
		record(),                             // no source attribute
		record(intAttr("team", 7)),           // source value that is no string
		record(stringAttr("team", team1)),    // no table entry
	})

	require.NoError(t, s.Notify(t.Context(), event))
	require.Len(t, out.events, 1)

	records := out.events[0].Logs.GetResourceLogs()[0].GetScopeLogs()[0].GetLogRecords()
	require.Len(t, records, 5)
	assert.Equal(t, "cart", records[0].GetAttributes()[1].GetValue().GetStringValue())
	for _, r := range records[1:] {
		values := map[string]string{}
		for _, kv := range r.GetAttributes() {
			values[kv.GetKey()] = kv.GetValue().GetStringValue()
		}
		assert.Equal(t, "unknown", values[teamIDKey], "a miss sets the default")
	}

	// nothing is filtered or dropped: every record arrives
	assert.Equal(t, 5, out.events[0].RecordCount())
}

func TestMixedBatch(t *testing.T) {
	t.Parallel()

	s, out := newStage(t, mappedSpec(map[string]string{team0: "cart", team1: "checkout"}))

	event := logs(
		[]*logspb.LogRecord{record(stringAttr("team", team0)), record(stringAttr("team", "team-9"))},
		[]*logspb.LogRecord{record(stringAttr("team", team1)), record()},
	)

	require.NoError(t, s.Notify(t.Context(), event))
	require.Len(t, out.events, 1)

	rls := out.events[0].Logs.GetResourceLogs()
	require.Len(t, rls, 2)
	first := rls[0].GetScopeLogs()[0].GetLogRecords()
	second := rls[1].GetScopeLogs()[0].GetLogRecords()

	assert.Equal(t, "cart", first[0].GetAttributes()[1].GetValue().GetStringValue())
	assert.Equal(t, "unknown", first[1].GetAttributes()[1].GetValue().GetStringValue())
	assert.Equal(t, "checkout", second[0].GetAttributes()[1].GetValue().GetStringValue())
	assert.Len(t, second[1].GetAttributes(), 1, "a record without attributes gets the target only")
	assert.Equal(t, "unknown", second[1].GetAttributes()[0].GetValue().GetStringValue())
	assert.Equal(t, 4, out.events[0].RecordCount())
}

func TestExistingTargetIsOverwritten(t *testing.T) {
	t.Parallel()

	s, out := newStage(t, mappedSpec(map[string]string{team0: "cart"}))

	event := logs([]*logspb.LogRecord{
		record(stringAttr("team", team0), stringAttr(teamIDKey, "stale")),
	})

	require.NoError(t, s.Notify(t.Context(), event))

	records := out.events[0].Logs.GetResourceLogs()[0].GetScopeLogs()[0].GetLogRecords()
	require.Len(t, records[0].GetAttributes(), 2, "the existing target is replaced, not appended")
	assert.Equal(t, teamIDKey, records[0].GetAttributes()[1].GetKey())
	assert.Equal(t, "cart", records[0].GetAttributes()[1].GetValue().GetStringValue())
}

func TestResourceLevel(t *testing.T) {
	t.Parallel()

	spec := mappedSpec(map[string]string{"shop": "ecommerce", "api": "platform"})
	spec["level"] = "resource"
	s, out := newStage(t, spec)

	shop := &logspb.ResourceLogs{
		Resource:  &resourcepb.Resource{Attributes: []*commonpb.KeyValue{stringAttr("team", "shop")}},
		ScopeLogs: []*logspb.ScopeLogs{{LogRecords: []*logspb.LogRecord{record()}}},
	}
	api := &logspb.ResourceLogs{
		Resource:  &resourcepb.Resource{Attributes: []*commonpb.KeyValue{stringAttr("team", "api")}},
		ScopeLogs: []*logspb.ScopeLogs{{LogRecords: []*logspb.LogRecord{record(), record()}}},
	}
	event := &domain.Event{Logs: &logspb.LogsData{ResourceLogs: []*logspb.ResourceLogs{shop, api}}}

	require.NoError(t, s.Notify(t.Context(), event))
	require.Len(t, out.events, 1)

	rls := out.events[0].Logs.GetResourceLogs()
	require.Len(t, rls, 2)
	for _, r := range rls[0].GetScopeLogs()[0].GetLogRecords() {
		assert.Len(t, r.GetAttributes(), 1)
		assert.Equal(t, "ecommerce", r.GetAttributes()[0].GetValue().GetStringValue())
	}
	for _, r := range rls[1].GetScopeLogs()[0].GetLogRecords() {
		assert.Len(t, r.GetAttributes(), 1)
		assert.Equal(t, "platform", r.GetAttributes()[0].GetValue().GetStringValue())
	}
}

func TestSignals(t *testing.T) {
	t.Parallel()

	s, out := newStage(t, mappedSpec(map[string]string{team0: "cart", team1: "checkout"}))

	gauge := &metricspb.Metric{
		Name: "gauge",
		Data: &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{DataPoints: []*metricspb.NumberDataPoint{
			{Attributes: []*commonpb.KeyValue{stringAttr("team", team1)}},
		}}},
	}
	sum := &metricspb.Metric{
		Name: "sum",
		Data: &metricspb.Metric_Sum{Sum: &metricspb.Sum{DataPoints: []*metricspb.NumberDataPoint{
			{Attributes: []*commonpb.KeyValue{intAttr("team", 3)}},
		}}},
	}
	histogram := &metricspb.Metric{
		Name: "histogram",
		Data: &metricspb.Metric_Histogram{Histogram: &metricspb.Histogram{DataPoints: []*metricspb.HistogramDataPoint{
			{Attributes: []*commonpb.KeyValue{stringAttr("team", team0)}, Count: 5, Min: new(1.0), Max: new(9.0)},
		}}},
	}
	exponential := &metricspb.Metric{
		Name: "exponential_histogram",
		Data: &metricspb.Metric_ExponentialHistogram{ExponentialHistogram: &metricspb.ExponentialHistogram{
			DataPoints: []*metricspb.ExponentialHistogramDataPoint{
				{Attributes: []*commonpb.KeyValue{stringAttr("team", team0)}, ZeroCount: 2},
			},
		}},
	}
	summary := &metricspb.Metric{
		Name: "summary",
		Data: &metricspb.Metric_Summary{Summary: &metricspb.Summary{DataPoints: []*metricspb.SummaryDataPoint{
			{Attributes: []*commonpb.KeyValue{stringAttr("team", team1)}, Count: 4, Sum: 10},
		}}},
	}
	event := &domain.Event{
		Logs: otlpgen.Logs(otlpgen.Shape{Records: 1, Resources: 1, Teams: 1}),
		Metrics: &metricspb.MetricsData{ResourceMetrics: []*metricspb.ResourceMetrics{{
			ScopeMetrics: []*metricspb.ScopeMetrics{{
				Metrics: []*metricspb.Metric{gauge, sum, histogram, exponential, summary},
			}},
		}}},
		Traces: &tracepb.TracesData{ResourceSpans: []*tracepb.ResourceSpans{{
			ScopeSpans: []*tracepb.ScopeSpans{{
				Spans: []*tracepb.Span{{Attributes: []*commonpb.KeyValue{stringAttr("team", team0)}, Name: "get"}},
			}},
		}}},
	}

	originalLogs, originalMetrics, originalTraces := proto.Clone(
		event.Logs,
	), proto.Clone(
		event.Metrics,
	), proto.Clone(
		event.Traces,
	)
	require.NoError(t, s.Notify(t.Context(), event))
	require.Len(t, out.events, 1)

	mappedMetrics := out.events[0].Metrics.GetResourceMetrics()[0].GetScopeMetrics()[0].GetMetrics()
	gaugeAttrs := mappedMetrics[0].GetGauge().GetDataPoints()[0].GetAttributes()
	assert.Equal(t, "checkout", gaugeAttrs[1].GetValue().GetStringValue())
	sumAttrs := mappedMetrics[1].GetSum().GetDataPoints()[0].GetAttributes()
	assert.Equal(t, "unknown", sumAttrs[1].GetValue().GetStringValue(), "a non-string source value has no entry")

	mappedHistogram := mappedMetrics[2].GetHistogram().GetDataPoints()[0]
	assert.Equal(t, "cart", mappedHistogram.GetAttributes()[1].GetValue().GetStringValue())
	assert.Equal(t, uint64(5), mappedHistogram.GetCount())
	assert.InDelta(t, 1.0, mappedHistogram.GetMin(), 0.0001)
	assert.InDelta(t, 9.0, mappedHistogram.GetMax(), 0.0001)

	mappedExponential := mappedMetrics[3].GetExponentialHistogram().GetDataPoints()[0]
	assert.Equal(t, "cart", mappedExponential.GetAttributes()[1].GetValue().GetStringValue())
	assert.Equal(t, uint64(2), mappedExponential.GetZeroCount())

	mappedSummary := mappedMetrics[4].GetSummary().GetDataPoints()[0]
	assert.Equal(t, "checkout", mappedSummary.GetAttributes()[1].GetValue().GetStringValue())
	assert.Equal(t, uint64(4), mappedSummary.GetCount())

	span := out.events[0].Traces.GetResourceSpans()[0].GetScopeSpans()[0].GetSpans()[0]
	assert.Equal(t, "cart", span.GetAttributes()[1].GetValue().GetStringValue())
	assert.Equal(t, "get", span.GetName())

	// the shared input is not modified
	assert.True(t, proto.Equal(originalLogs, event.Logs), "the shared logs must not be modified")
	assert.True(t, proto.Equal(originalMetrics, event.Metrics), "the shared metrics must not be modified")
	assert.True(t, proto.Equal(originalTraces, event.Traces), "the shared traces must not be modified")
}

func TestInputStaysUntouched(t *testing.T) {
	t.Parallel()

	s, out := newStage(t, mappedSpec(map[string]string{team0: "cart"}))

	event := logs(
		[]*logspb.LogRecord{record(stringAttr("team", team0), stringAttr(teamIDKey, "stale")), record()},
	)
	original := proto.Clone(event.Logs)

	require.NoError(t, s.Notify(t.Context(), event))
	require.Len(t, out.events, 1)

	assert.True(t, proto.Equal(original, event.Logs), "the shared input must not be modified")
	assert.NotSame(t, event, out.events[0], "subscribers receive a copy")
	assert.Same(
		t,
		event.Logs.GetResourceLogs()[0].GetResource(),
		out.events[0].Logs.GetResourceLogs()[0].GetResource(),
		"the unannotated resource is shared",
	)
	assert.NotSame(
		t,
		event.Logs.GetResourceLogs()[0].GetScopeLogs()[0].GetLogRecords()[0],
		out.events[0].Logs.GetResourceLogs()[0].GetScopeLogs()[0].GetLogRecords()[0],
		"the annotated record is a new one",
	)

	// a second sibling branch sees the same untouched event
	s2, out2 := newStage(t, mappedSpec(map[string]string{team0: "other"}))
	require.NoError(t, s2.Notify(t.Context(), event))
	assert.True(t, proto.Equal(original, event.Logs), "the shared input must not be modified")
	mapped, _ := attr(out2.events[0], teamIDKey)
	assert.Equal(t, "other", mapped)
}
