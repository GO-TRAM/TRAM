package nats

import (
	"fmt"
	"testing"

	"codeberg.org/eljoth/tram/internal/adapters/sink/base"
	"codeberg.org/eljoth/tram/internal/core/domain"
	"codeberg.org/eljoth/tram/internal/testutil/otlpgen"
	"github.com/go-playground/validator/v10"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	"google.golang.org/protobuf/proto"
)

// newSink builds a sink without a NATS connection for the given subject spec.
func newSink(t testing.TB, subject map[string]any) *NATS {
	t.Helper()
	subject["register"] = []any{">"}
	sinkBase, err := base.New[Spec]("nats", "in", map[string]any{
		"url":     "nats://localhost:4222",
		"stream":  "test",
		"subject": subject,
	}, validator.New())
	require.NoError(t, err)
	logs, metrics, traces, err := compileSubjects(sinkBase.Spec().Subject)
	require.NoError(t, err)
	return &NATS{Sink: sinkBase, logs: logs, metrics: metrics, traces: traces}
}

func attr(attrs []*commonpb.KeyValue, key string) string {
	for _, a := range attrs {
		if a.Key == key {
			return a.Value.GetStringValue()
		}
	}
	return ""
}

// contents describes what grouping produced as subject -> signal -> entries.
// Each entry is "<service.name of its resource>/<scope name>/<entry name>",
// so the tests also check that entries keep their resource and scope.
func contents(t *testing.T, messages []message) map[string]map[domain.Signal][]string {
	t.Helper()
	out := map[string]map[domain.Signal][]string{}
	for _, m := range messages {
		signal, _, err := m.event.MarshalOTLPData()
		require.NoError(t, err)
		require.Equal(t, 1, countSignals(m.event), "a message carries exactly one signal type")
		if out[m.subject] == nil {
			out[m.subject] = map[domain.Signal][]string{}
		}
		for _, rl := range m.event.Logs.GetResourceLogs() {
			for _, sl := range rl.ScopeLogs {
				for _, r := range sl.LogRecords {
					out[m.subject][signal] = append(out[m.subject][signal], fmt.Sprintf("%s/%s/%s", attr(rl.Resource.GetAttributes(), "service.name"), sl.Scope.GetName(), r.Body.GetStringValue()))
				}
			}
		}
		for _, rm := range m.event.Metrics.GetResourceMetrics() {
			for _, sm := range rm.ScopeMetrics {
				for _, metric := range sm.Metrics {
					out[m.subject][signal] = append(out[m.subject][signal], fmt.Sprintf("%s/%s/%s", attr(rm.Resource.GetAttributes(), "service.name"), sm.Scope.GetName(), metric.Name))
				}
			}
		}
		for _, rs := range m.event.Traces.GetResourceSpans() {
			for _, ss := range rs.ScopeSpans {
				for _, s := range ss.Spans {
					out[m.subject][signal] = append(out[m.subject][signal], fmt.Sprintf("%s/%s/%s", attr(rs.Resource.GetAttributes(), "service.name"), ss.Scope.GetName(), s.Name))
				}
			}
		}
	}
	return out
}

func countSignals(e *domain.Event) int {
	n := 0
	if e.Logs != nil {
		n++
	}
	if e.Metrics != nil {
		n++
	}
	if e.Traces != nil {
		n++
	}
	return n
}

const (
	logsByTeam    = "'logs.' + logs.resource_logs[0].scope_logs[0].log_records[0].attributes.get_attr('team')"
	metricsByTeam = "'metrics.' + metrics.resource_metrics[0].scope_metrics[0].metrics[0].gauge.data_points[0].attributes.get_attr('team')"
	tracesByTeam  = "'traces.' + traces.resource_spans[0].scope_spans[0].spans[0].attributes.get_attr('team')"
)

func TestGroupEventsBySubjects(t *testing.T) {
	// 4 records over 2 resources, alternating between team-0 and team-1.
	shape := otlpgen.Shape{Records: 4, Resources: 2, Teams: 2}

	cases := []struct {
		name     string
		subject  map[string]any
		event    *domain.Event
		expected map[string]map[domain.Signal][]string
	}{
		{
			name:    "logs to one subject",
			subject: map[string]any{"logs": "'logs.all'"},
			event:   &domain.Event{Logs: otlpgen.Logs(shape)},
			expected: map[string]map[domain.Signal][]string{
				"logs.all": {domain.SignalLogs: {"service-0/otlpgen/log message 0", "service-0/otlpgen/log message 1", "service-1/otlpgen/log message 2", "service-1/otlpgen/log message 3"}},
			},
		},
		{
			name:    "logs split by record attribute",
			subject: map[string]any{"logs": logsByTeam},
			event:   &domain.Event{Logs: otlpgen.Logs(shape)},
			expected: map[string]map[domain.Signal][]string{
				"logs.team-0": {domain.SignalLogs: {"service-0/otlpgen/log message 0", "service-1/otlpgen/log message 2"}},
				"logs.team-1": {domain.SignalLogs: {"service-0/otlpgen/log message 1", "service-1/otlpgen/log message 3"}},
			},
		},
		{
			name:    "metrics split by data point attribute",
			subject: map[string]any{"metrics": metricsByTeam},
			event:   &domain.Event{Metrics: otlpgen.Metrics(shape)},
			expected: map[string]map[domain.Signal][]string{
				"metrics.team-0": {domain.SignalMetrics: {"service-0/otlpgen/metric_0", "service-1/otlpgen/metric_2"}},
				"metrics.team-1": {domain.SignalMetrics: {"service-0/otlpgen/metric_1", "service-1/otlpgen/metric_3"}},
			},
		},
		{
			name:    "spans split by attribute",
			subject: map[string]any{"traces": tracesByTeam},
			event:   &domain.Event{Traces: otlpgen.Traces(shape)},
			expected: map[string]map[domain.Signal][]string{
				"traces.team-0": {domain.SignalTraces: {"service-0/otlpgen/span-0", "service-1/otlpgen/span-2"}},
				"traces.team-1": {domain.SignalTraces: {"service-0/otlpgen/span-1", "service-1/otlpgen/span-3"}},
			},
		},
		{
			name:    "signals without expression are not published",
			subject: map[string]any{"logs": "'logs.all'"},
			event:   &domain.Event{Logs: otlpgen.Logs(otlpgen.Shape{Records: 1, Resources: 1}), Traces: otlpgen.Traces(shape)},
			expected: map[string]map[domain.Signal][]string{
				"logs.all": {domain.SignalLogs: {"service-0/otlpgen/log message 0"}},
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			sink := newSink(t, c.subject)
			original := proto.Clone(c.event.Logs)

			messages, err := sink.groupEventsBySubjects(c.event)
			require.NoError(t, err)
			assert.Equal(t, c.expected, contents(t, messages))
			if c.event.Logs != nil {
				assert.True(t, proto.Equal(original, c.event.Logs), "the shared event must not be modified")
			}
		})
	}
}

// Logs and metrics resolving to the same subject are published as separate
// messages, because a message carries one signal type.
func TestGroupEventsBySubjectsSameSubjectForSeveralSignals(t *testing.T) {
	shape := otlpgen.Shape{Records: 2, Resources: 1, Teams: 1}
	sink := newSink(t, map[string]any{"logs": "'shared'", "metrics": "'shared'"})

	messages, err := sink.groupEventsBySubjects(&domain.Event{Logs: otlpgen.Logs(shape), Metrics: otlpgen.Metrics(shape)})
	require.NoError(t, err)

	assert.Len(t, messages, 2)
	assert.Equal(t, map[string]map[domain.Signal][]string{
		"shared": {
			domain.SignalLogs:    {"service-0/otlpgen/log message 0", "service-0/otlpgen/log message 1"},
			domain.SignalMetrics: {"service-0/otlpgen/metric_0", "service-0/otlpgen/metric_1"},
		},
	}, contents(t, messages))
}

func TestGroupEventsBySubjectsRequiresStringSubject(t *testing.T) {
	sink := newSink(t, map[string]any{"logs": "size(logs.resource_logs)"})
	_, err := sink.groupEventsBySubjects(&domain.Event{Logs: otlpgen.Logs(otlpgen.Shape{Records: 1, Resources: 1})})
	assert.Error(t, err)
}

func TestGroupEventsBySubjectsKeepsMissingResourceAndScope(t *testing.T) {
	sink := newSink(t, map[string]any{"logs": "'logs.all'"})
	record := &logspb.LogRecord{Body: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "bare"}}}
	event := &domain.Event{Logs: &logspb.LogsData{ResourceLogs: []*logspb.ResourceLogs{{ScopeLogs: []*logspb.ScopeLogs{{LogRecords: []*logspb.LogRecord{record}}}}}}}

	messages, err := sink.groupEventsBySubjects(event)
	require.NoError(t, err)
	require.Len(t, messages, 1)
	rl := messages[0].event.Logs.ResourceLogs[0]
	assert.Nil(t, rl.Resource)
	assert.Nil(t, rl.ScopeLogs[0].Scope)
	assert.Equal(t, []*logspb.LogRecord{record}, rl.ScopeLogs[0].LogRecords)
}

func TestGroupEventsBySubjectsSharesData(t *testing.T) {
	data := otlpgen.Logs(otlpgen.Shape{Records: 4, Resources: 2, Teams: 2})

	t.Run("one subject publishes the payload as it is", func(t *testing.T) {
		messages, err := newSink(t, map[string]any{"logs": "'logs.all'"}).groupEventsBySubjects(&domain.Event{Logs: data})
		require.NoError(t, err)
		require.Len(t, messages, 1)
		assert.Same(t, data, messages[0].event.Logs)
	})

	t.Run("split payloads share records, resources and scopes", func(t *testing.T) {
		messages, err := newSink(t, map[string]any{"logs": logsByTeam}).groupEventsBySubjects(&domain.Event{Logs: data})
		require.NoError(t, err)
		require.Len(t, messages, 2)
		got := messages[0].event.Logs.ResourceLogs[0]
		assert.Same(t, data.ResourceLogs[0].Resource, got.Resource)
		assert.Same(t, data.ResourceLogs[0].ScopeLogs[0].Scope, got.ScopeLogs[0].Scope)
		assert.Same(t, data.ResourceLogs[0].ScopeLogs[0].LogRecords[0], got.ScopeLogs[0].LogRecords[0])
	})
}

func TestGroupEventsBySubjectsOrder(t *testing.T) {
	// Subjects from the resource: the payload the expression sees is reused
	// per scope, so this also checks it carries the right resource.
	sink := newSink(t, map[string]any{"logs": "'logs.' + logs.resource_logs[0].resource.attributes.get_attr('service.name')"})
	data := otlpgen.Logs(otlpgen.Shape{Records: 6, Resources: 3, Teams: 1})

	messages, err := sink.groupEventsBySubjects(&domain.Event{Logs: data})
	require.NoError(t, err)

	var subjects []string
	for _, m := range messages {
		subjects = append(subjects, m.subject)
		assert.Equal(t, 2, m.event.RecordCount())
	}
	assert.Equal(t, []string{"logs.service-0", "logs.service-1", "logs.service-2"}, subjects, "subjects in order of first occurrence")
}

// Resources are matched by their entry, not by content, so equal resources
// from separate entries (e.g. merged by a batch stage) stay separate.
func TestGroupEventsBySubjectsKeepsSeparateResourceEntries(t *testing.T) {
	first := otlpgen.Logs(otlpgen.Shape{Records: 2, Resources: 1, Teams: 2})
	second := otlpgen.Logs(otlpgen.Shape{Records: 2, Resources: 1, Teams: 2})
	merged := &logspb.LogsData{ResourceLogs: append(first.ResourceLogs, second.ResourceLogs...)}

	messages, err := newSink(t, map[string]any{"logs": logsByTeam}).groupEventsBySubjects(&domain.Event{Logs: merged})
	require.NoError(t, err)
	require.Len(t, messages, 2)
	for _, m := range messages {
		assert.Len(t, m.event.Logs.ResourceLogs, 2)
		assert.Equal(t, 2, m.event.RecordCount())
	}
}
