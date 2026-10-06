package celfilter_test

import (
	"context"
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

	"codeberg.org/eljoth/tram/internal/adapters/stage/celfilter"
	"codeberg.org/eljoth/tram/internal/core/domain"
	"codeberg.org/eljoth/tram/internal/testutil/otlpgen"
)

type capture struct{ events []*domain.Event }

func (c *capture) Name() string { return "capture" }
func (c *capture) From() string { return "filter" }
func (c *capture) Stop() error  { return nil }
func (c *capture) Notify(_ context.Context, e *domain.Event) error {
	c.events = append(c.events, e)
	return nil
}

func newFilter(t *testing.T, spec map[string]any) (*celfilter.CELFilter, *capture) {
	t.Helper()
	f, err := celfilter.New(domain.StageSpec{Name: "filter", From: "source", Spec: spec}, validator.New())
	require.NoError(t, err)
	c := &capture{}
	require.NoError(t, f.Subscribe(c))
	return f, c
}

func stringAttr(key, value string) *commonpb.KeyValue {
	return &commonpb.KeyValue{
		Key:   key,
		Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: value}},
	}
}

func customerLogs(customers ...string) *domain.Event {
	sl := &logspb.ScopeLogs{}
	for _, c := range customers {
		sl.LogRecords = append(
			sl.LogRecords,
			&logspb.LogRecord{Attributes: []*commonpb.KeyValue{stringAttr("customer", c)}},
		)
	}
	return &domain.Event{Logs: &logspb.LogsData{ResourceLogs: []*logspb.ResourceLogs{{
		Resource:  &resourcepb.Resource{Attributes: []*commonpb.KeyValue{stringAttr("service.name", "shop")}},
		ScopeLogs: []*logspb.ScopeLogs{sl},
	}}}}
}

func customers(e *domain.Event) []string {
	var out []string
	for _, rl := range e.Logs.GetResourceLogs() {
		for _, sl := range rl.GetScopeLogs() {
			for _, r := range sl.GetLogRecords() {
				out = append(out, r.GetAttributes()[0].GetValue().GetStringValue())
			}
		}
	}
	return out
}

func TestNew(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name        string
		spec        map[string]any
		expectErr   bool
		expectErrIs error
	}{
		{name: "logs", spec: map[string]any{"logs": "record.severity_text == 'ERROR'"}},
		{
			name: "metrics on data points",
			spec: map[string]any{"metrics": "datapoint.attributes.exists(a, a.key == 'team')"},
		},
		{name: "traces", spec: map[string]any{"traces": "span.name.startsWith('GET')"}},
		{name: "no expression", spec: map[string]any{}, expectErr: true},
		{
			name:        "invalid logs",
			spec:        map[string]any{"logs": "record.unknown_field"},
			expectErr:   true,
			expectErrIs: celfilter.ErrCELInvalidLogsExpression,
		},
		{
			name:        "non-bool logs",
			spec:        map[string]any{"logs": "record.severity_text"},
			expectErr:   true,
			expectErrIs: celfilter.ErrCELInvalidLogsExpression,
		},
		{
			name:        "invalid metrics",
			spec:        map[string]any{"metrics": "metric.name +"},
			expectErr:   true,
			expectErrIs: celfilter.ErrCELInvalidMetricsExpression,
		},
		{
			name:        "wrong variable for traces",
			spec:        map[string]any{"traces": "record.severity_text == 'x'"},
			expectErr:   true,
			expectErrIs: celfilter.ErrCELInvalidTracesExpression,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			_, err := celfilter.New(domain.StageSpec{Name: "filter", Spec: c.spec}, validator.New())
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

func TestSplitCustomersFromOneScope(t *testing.T) {
	t.Parallel()

	filterA, outA := newFilter(
		t,
		map[string]any{"logs": "record.attributes.exists(a, a.key == 'customer' && a.value.string_value == 'A')"},
	)
	filterB, outB := newFilter(
		t,
		map[string]any{"logs": "!record.attributes.exists(a, a.key == 'customer' && a.value.string_value == 'A')"},
	)

	event := customerLogs("A", "B", "A", "C")
	original := proto.Clone(event.Logs)

	require.NoError(t, filterA.Notify(t.Context(), event))
	require.NoError(t, filterB.Notify(t.Context(), event))

	require.Len(t, outA.events, 1)
	require.Len(t, outB.events, 1)
	assert.Equal(t, []string{"A", "A"}, customers(outA.events[0]))
	assert.Equal(t, []string{"B", "C"}, customers(outB.events[0]))
	assert.Same(
		t,
		event.Logs.GetResourceLogs()[0].GetResource(),
		outA.events[0].Logs.GetResourceLogs()[0].GetResource(),
	)
	assert.True(t, proto.Equal(original, event.Logs), "the shared input must not be modified")
}

func TestNotify(t *testing.T) {
	t.Parallel()

	shape := otlpgen.Shape{Records: 8, Resources: 2, Teams: 2}

	cases := []struct {
		name          string
		spec          map[string]any
		event         func() *domain.Event
		expectRecords int
		expectSame    bool
		expectNone    bool
	}{
		{
			name: "logs by record attribute",
			spec: map[string]any{
				"logs": "record.attributes.exists(a, a.key == 'team' && a.value.string_value == 'team-0')",
			},
			event:         func() *domain.Event { return &domain.Event{Logs: otlpgen.Logs(shape)} },
			expectRecords: 4,
		},
		{
			name: "logs by resource attribute",
			spec: map[string]any{
				"logs": "resource.attributes.exists(a, a.key == 'service.name' && a.value.string_value == 'service-1')",
			},
			event:         func() *domain.Event { return &domain.Event{Logs: otlpgen.Logs(shape)} },
			expectRecords: 4,
		},
		{
			name:          "logs by scope",
			spec:          map[string]any{"logs": "scope.name == 'otlpgen'"},
			event:         func() *domain.Event { return &domain.Event{Logs: otlpgen.Logs(shape)} },
			expectRecords: 8,
			expectSame:    true,
		},
		{
			name: "metric data points by attribute",
			spec: map[string]any{
				"metrics": "datapoint.attributes.exists(a, a.key == 'team' && a.value.string_value == 'team-1')",
			},
			event:         func() *domain.Event { return &domain.Event{Metrics: otlpgen.Metrics(shape)} },
			expectRecords: 4,
		},
		{
			name:          "metrics by name",
			spec:          map[string]any{"metrics": "metric.name == 'metric_0'"},
			event:         func() *domain.Event { return &domain.Event{Metrics: otlpgen.Metrics(shape)} },
			expectRecords: 1,
		},
		{
			name:          "spans",
			spec:          map[string]any{"traces": "span.name in ['span-1', 'span-2']"},
			event:         func() *domain.Event { return &domain.Event{Traces: otlpgen.Traces(shape)} },
			expectRecords: 2,
		},
		{
			name:       "nothing kept forwards nothing",
			spec:       map[string]any{"logs": "false"},
			event:      func() *domain.Event { return &domain.Event{Logs: otlpgen.Logs(shape)} },
			expectNone: true,
		},
		{
			name: "signals without expression pass unfiltered",
			spec: map[string]any{"logs": "false"},
			event: func() *domain.Event {
				return &domain.Event{Logs: otlpgen.Logs(shape), Traces: otlpgen.Traces(shape)}
			},
			expectRecords: 8,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			f, out := newFilter(t, c.spec)
			event := c.event()

			require.NoError(t, f.Notify(t.Context(), event))
			if c.expectNone {
				assert.Empty(t, out.events)
				return
			}
			require.Len(t, out.events, 1)
			assert.Equal(t, c.expectRecords, out.events[0].RecordCount())
			if c.expectSame {
				assert.Same(t, event, out.events[0], "an event that is kept completely is passed on as is")
			}
		})
	}
}

func TestDocumentedExamples(t *testing.T) {
	t.Parallel()

	logs := func(severities ...string) *domain.Event {
		sl := &logspb.ScopeLogs{}
		for _, s := range severities {
			sl.LogRecords = append(sl.LogRecords, &logspb.LogRecord{SeverityText: s})
		}
		return &domain.Event{
			Logs: &logspb.LogsData{ResourceLogs: []*logspb.ResourceLogs{{ScopeLogs: []*logspb.ScopeLogs{sl}}}},
		}
	}
	gauge := func(name string) *metricspb.Metric {
		return &metricspb.Metric{
			Name: name,
			Data: &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{DataPoints: []*metricspb.NumberDataPoint{{}}}},
		}
	}
	service := func(name string) *resourcepb.Resource {
		return &resourcepb.Resource{Attributes: []*commonpb.KeyValue{stringAttr("service.name", name)}}
	}

	cases := []struct {
		name          string
		spec          map[string]any
		event         *domain.Event
		expectRecords int
	}{
		{
			name:          "keep only ERROR logs",
			spec:          map[string]any{"logs": "record.severity_text == 'ERROR'"},
			event:         logs("ERROR", "INFO", "INFO", "ERROR"),
			expectRecords: 2,
		},
		{
			name: "keep only the data points of one metric",
			spec: map[string]any{"metrics": "metric.name == 'http.server.request.duration'"},
			event: &domain.Event{Metrics: &metricspb.MetricsData{ResourceMetrics: []*metricspb.ResourceMetrics{
				{
					ScopeMetrics: []*metricspb.ScopeMetrics{
						{Metrics: []*metricspb.Metric{gauge("http.server.request.duration"), gauge("other")}},
					},
				},
			}}},
			expectRecords: 1,
		},
		{
			name: "keep only spans of one service",
			spec: map[string]any{
				"traces": "resource.attributes.exists(a, a.key == 'service.name' && a.value.string_value == 'checkout')",
			},
			event: &domain.Event{Traces: &tracepb.TracesData{ResourceSpans: []*tracepb.ResourceSpans{
				{Resource: service("checkout"), ScopeSpans: []*tracepb.ScopeSpans{{Spans: []*tracepb.Span{{}, {}}}}},
				{Resource: service("cart"), ScopeSpans: []*tracepb.ScopeSpans{{Spans: []*tracepb.Span{{}}}}},
			}}},
			expectRecords: 2,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			f, out := newFilter(t, c.spec)
			require.NoError(t, f.Notify(t.Context(), c.event))
			require.Len(t, out.events, 1)
			assert.Equal(t, c.expectRecords, out.events[0].RecordCount())
		})
	}
}

func TestEvaluationErrorRejectsEvent(t *testing.T) {
	t.Parallel()

	f, out := newFilter(t, map[string]any{"logs": "record.attributes[5].key == 'x'"})

	err := f.Notify(t.Context(), customerLogs("A"))
	require.ErrorIs(t, err, domain.ErrEventRejected)
	require.ErrorIs(t, err, celfilter.ErrCELEvaluation)
	assert.Empty(t, out.events)
}

func TestMissingResourceAndScope(t *testing.T) {
	t.Parallel()

	f, out := newFilter(t, map[string]any{"logs": "!resource.attributes.exists(a, a.key == 'x') && scope.name == ''"})
	event := &domain.Event{Logs: &logspb.LogsData{ResourceLogs: []*logspb.ResourceLogs{{
		ScopeLogs: []*logspb.ScopeLogs{{LogRecords: []*logspb.LogRecord{{}}}},
	}}}}

	require.NoError(t, f.Notify(t.Context(), event))
	require.Len(t, out.events, 1)
	assert.Equal(t, 1, out.events[0].RecordCount())
}

func TestHistogramDataPoints(t *testing.T) {
	t.Parallel()

	f, out := newFilter(t, map[string]any{"metrics": "datapoint.count > 1u"})
	event := &domain.Event{Metrics: &metricspb.MetricsData{ResourceMetrics: []*metricspb.ResourceMetrics{{
		ScopeMetrics: []*metricspb.ScopeMetrics{{Metrics: []*metricspb.Metric{{
			Name: "latency",
			Data: &metricspb.Metric_Histogram{Histogram: &metricspb.Histogram{
				DataPoints: []*metricspb.HistogramDataPoint{{Count: 1}, {Count: 5}, {Count: 9}},
			}},
		}}}},
	}}}}

	require.NoError(t, f.Notify(t.Context(), event))
	require.Len(t, out.events, 1)
	assert.Equal(t, 2, out.events[0].RecordCount())
}
