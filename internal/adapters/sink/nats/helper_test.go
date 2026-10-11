// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package nats_test

import (
	"fmt"
	"testing"

	"github.com/go-playground/validator/v10"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/protobuf/proto"

	natssink "codeberg.org/eljoth/tram/internal/adapters/sink/nats"
	"codeberg.org/eljoth/tram/internal/core/domain"
	"codeberg.org/eljoth/tram/internal/testutil/otlpgen"
)

func newSink(t testing.TB, subject map[string]any) *natssink.NATS {
	t.Helper()
	subject["register"] = []any{">"}
	sink, err := natssink.NewWithoutConnection(map[string]any{
		"url":     "nats://localhost:4222",
		"stream":  "test",
		"subject": subject,
	}, validator.New())
	require.NoError(t, err)
	return sink
}

func attr(attrs []*commonpb.KeyValue, key string) string {
	for _, a := range attrs {
		if a.GetKey() == key {
			return a.GetValue().GetStringValue()
		}
	}
	return ""
}

func contents(t *testing.T, messages []natssink.Message) map[string]map[domain.Signal][]string {
	t.Helper()
	out := map[string]map[domain.Signal][]string{}
	for _, m := range messages {
		signal, _, err := m.Event.MarshalOTLPData()
		require.NoError(t, err)
		require.Equal(t, 1, countSignals(m.Event), "a message carries exactly one signal type")
		if out[m.Subject] == nil {
			out[m.Subject] = map[domain.Signal][]string{}
		}
		out[m.Subject][signal] = append(out[m.Subject][signal], entries(m.Event)...)
	}
	return out
}

func entries(event *domain.Event) []string {
	var out []string
	entry := func(resource []*commonpb.KeyValue, scope, name string) {
		out = append(out, fmt.Sprintf("%s/%s/%s", attr(resource, "service.name"), scope, name))
	}
	for _, rl := range event.Logs.GetResourceLogs() {
		for _, sl := range rl.GetScopeLogs() {
			for _, r := range sl.GetLogRecords() {
				entry(rl.GetResource().GetAttributes(), sl.GetScope().GetName(), r.GetBody().GetStringValue())
			}
		}
	}
	for _, rm := range event.Metrics.GetResourceMetrics() {
		for _, sm := range rm.GetScopeMetrics() {
			for _, metric := range sm.GetMetrics() {
				entry(rm.GetResource().GetAttributes(), sm.GetScope().GetName(), metric.GetName())
			}
		}
	}
	for _, rs := range event.Traces.GetResourceSpans() {
		for _, ss := range rs.GetScopeSpans() {
			for _, span := range ss.GetSpans() {
				entry(rs.GetResource().GetAttributes(), ss.GetScope().GetName(), span.GetName())
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

	logsByTeamNew    = "'logs.' + record.attributes.get_attr('team')"
	metricsByTeamNew = "'metrics.' + metric.name"
	tracesByTeamNew  = "'traces.' + span.attributes.get_attr('team')"
)

func TestGroupEventsBySubjects(t *testing.T) {
	t.Parallel()

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
				"logs.all": {
					domain.SignalLogs: {
						"service-0/otlpgen/log message 0",
						"service-0/otlpgen/log message 1",
						"service-1/otlpgen/log message 2",
						"service-1/otlpgen/log message 3",
					},
				},
			},
		},
		{
			name:    "logs split by record attribute",
			subject: map[string]any{"logs": logsByTeam},
			event:   &domain.Event{Logs: otlpgen.Logs(shape)},
			expected: map[string]map[domain.Signal][]string{
				"logs.team-0": {
					domain.SignalLogs: {"service-0/otlpgen/log message 0", "service-1/otlpgen/log message 2"},
				},
				"logs.team-1": {
					domain.SignalLogs: {"service-0/otlpgen/log message 1", "service-1/otlpgen/log message 3"},
				},
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
			name:    "logs split by record attribute with new variables",
			subject: map[string]any{"logs": logsByTeamNew},
			event:   &domain.Event{Logs: otlpgen.Logs(shape)},
			expected: map[string]map[domain.Signal][]string{
				"logs.team-0": {
					domain.SignalLogs: {"service-0/otlpgen/log message 0", "service-1/otlpgen/log message 2"},
				},
				"logs.team-1": {
					domain.SignalLogs: {"service-0/otlpgen/log message 1", "service-1/otlpgen/log message 3"},
				},
			},
		},
		{
			name:    "traces split by attribute with new variables",
			subject: map[string]any{"traces": tracesByTeamNew},
			event:   &domain.Event{Traces: otlpgen.Traces(shape)},
			expected: map[string]map[domain.Signal][]string{
				"traces.team-0": {domain.SignalTraces: {"service-0/otlpgen/span-0", "service-1/otlpgen/span-2"}},
				"traces.team-1": {domain.SignalTraces: {"service-0/otlpgen/span-1", "service-1/otlpgen/span-3"}},
			},
		},
		{
			name:    "metrics with new variables",
			subject: map[string]any{"metrics": "'metrics.' + metric.name"},
			event:   &domain.Event{Metrics: otlpgen.Metrics(shape)},
			expected: map[string]map[domain.Signal][]string{
				"metrics.metric_0": {domain.SignalMetrics: {"service-0/otlpgen/metric_0"}},
				"metrics.metric_1": {domain.SignalMetrics: {"service-0/otlpgen/metric_1"}},
				"metrics.metric_2": {domain.SignalMetrics: {"service-1/otlpgen/metric_2"}},
				"metrics.metric_3": {domain.SignalMetrics: {"service-1/otlpgen/metric_3"}},
			},
		},
		{
			name: "logs split by resource and scope variables",
			subject: map[string]any{
				"logs": "'logs.' + scope.name + '.' + resource.attributes.get_attr('service.name')",
			},
			event: &domain.Event{Logs: otlpgen.Logs(shape)},
			expected: map[string]map[domain.Signal][]string{
				"logs.otlpgen.service-0": {
					domain.SignalLogs: {"service-0/otlpgen/log message 0", "service-0/otlpgen/log message 1"},
				},
				"logs.otlpgen.service-1": {
					domain.SignalLogs: {"service-1/otlpgen/log message 2", "service-1/otlpgen/log message 3"},
				},
			},
		},
		{
			name:    "metrics split by resource variable",
			subject: map[string]any{"metrics": "'metrics.' + resource.attributes.get_attr('service.name')"},
			event:   &domain.Event{Metrics: otlpgen.Metrics(shape)},
			expected: map[string]map[domain.Signal][]string{
				"metrics.service-0": {
					domain.SignalMetrics: {"service-0/otlpgen/metric_0", "service-0/otlpgen/metric_1"},
				},
				"metrics.service-1": {
					domain.SignalMetrics: {"service-1/otlpgen/metric_2", "service-1/otlpgen/metric_3"},
				},
			},
		},
		{
			name:    "traces split by resource variable",
			subject: map[string]any{"traces": "'traces.' + resource.attributes.get_attr('service.name')"},
			event:   &domain.Event{Traces: otlpgen.Traces(shape)},
			expected: map[string]map[domain.Signal][]string{
				"traces.service-0": {domain.SignalTraces: {"service-0/otlpgen/span-0", "service-0/otlpgen/span-1"}},
				"traces.service-1": {domain.SignalTraces: {"service-1/otlpgen/span-2", "service-1/otlpgen/span-3"}},
			},
		},
		{
			name:    "signals without expression are not published",
			subject: map[string]any{"logs": "'logs.all'"},
			event: &domain.Event{
				Logs:   otlpgen.Logs(otlpgen.Shape{Records: 1, Resources: 1}),
				Traces: otlpgen.Traces(shape),
			},
			expected: map[string]map[domain.Signal][]string{
				"logs.all": {domain.SignalLogs: {"service-0/otlpgen/log message 0"}},
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			sink := newSink(t, c.subject)
			original := proto.Clone(c.event.Logs)

			messages, err := natssink.GroupEventsBySubjects(sink, c.event)
			require.NoError(t, err)
			assert.Equal(t, c.expected, contents(t, messages))
			if c.event.Logs != nil {
				assert.True(t, proto.Equal(original, c.event.Logs), "the shared event must not be modified")
			}
		})
	}
}

func TestGroupEventsBySubjectsSameSubjectForSeveralSignals(t *testing.T) {
	t.Parallel()

	shape := otlpgen.Shape{Records: 2, Resources: 1, Teams: 1}
	sink := newSink(t, map[string]any{"logs": "'shared'", "metrics": "'shared'"})

	messages, err := natssink.GroupEventsBySubjects(sink,
		&domain.Event{Logs: otlpgen.Logs(shape), Metrics: otlpgen.Metrics(shape)},
	)
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
	t.Parallel()

	sink := newSink(t, map[string]any{"logs": "size(logs.resource_logs)"})
	_, err := natssink.GroupEventsBySubjects(
		sink,
		&domain.Event{Logs: otlpgen.Logs(otlpgen.Shape{Records: 1, Resources: 1})},
	)
	assert.Error(t, err)
}

func TestGroupEventsBySubjectsWithoutResourceAndScope(t *testing.T) {
	t.Parallel()

	sink := newSink(t, map[string]any{
		"logs": "'logs.' + string(size(resource.attributes)) + '.' + (scope.name == '' ? 'none' : scope.name)",
	})
	event := &domain.Event{Logs: &logspb.LogsData{ResourceLogs: []*logspb.ResourceLogs{{
		ScopeLogs: []*logspb.ScopeLogs{{LogRecords: []*logspb.LogRecord{{}}}},
	}}}}

	messages, err := natssink.GroupEventsBySubjects(sink, event)
	require.NoError(t, err)
	require.Len(t, messages, 1)
	assert.Equal(t, "logs.0.none", messages[0].Subject)
}

func TestNotifyRejectsUnresolvableSubject(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		subject string
	}{
		{name: "evaluation fails", subject: "'logs.' + logs.resource_logs[0].scope_logs[1].scope.name"},
		{name: "missing attribute", subject: "'logs.' + logs.resource_logs[0].resource.attributes.get_attr('team')"},
		{name: "whitespace", subject: "'logs.' + logs.resource_logs[0].resource.attributes.get_attr('service.name')"},
		{name: "wildcard", subject: "'logs.>'"},
	}

	event := &domain.Event{Logs: &logspb.LogsData{ResourceLogs: []*logspb.ResourceLogs{{
		Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{{
			Key:   "service.name",
			Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "my shop"}},
		}}},
		ScopeLogs: []*logspb.ScopeLogs{{LogRecords: []*logspb.LogRecord{{}}}},
	}}}}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			sink := newSink(t, map[string]any{"logs": c.subject})
			assert.ErrorIs(t, sink.Notify(t.Context(), event), domain.ErrEventRejected)
		})
	}
}

func TestSubjectRejectsUnknownVariable(t *testing.T) {
	t.Parallel()

	_, err := natssink.NewWithoutConnection(map[string]any{
		"url":    "nats://localhost:4222",
		"stream": "test",
		"subject": map[string]any{
			"register": []any{">"},
			"logs":     "'logs.' + string(event.Logs != null)",
		},
	}, validator.New())
	assert.ErrorIs(t, err, natssink.ErrCELInvalidLogsExpression)
}

func TestGroupEventsBySubjectsKeepsMissingResourceAndScope(t *testing.T) {
	t.Parallel()

	sink := newSink(t, map[string]any{"logs": "'logs.all'"})
	record := &logspb.LogRecord{Body: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "bare"}}}
	event := &domain.Event{
		Logs: &logspb.LogsData{
			ResourceLogs: []*logspb.ResourceLogs{
				{ScopeLogs: []*logspb.ScopeLogs{{LogRecords: []*logspb.LogRecord{record}}}},
			},
		},
	}

	messages, err := natssink.GroupEventsBySubjects(sink, event)
	require.NoError(t, err)
	require.Len(t, messages, 1)
	rl := messages[0].Event.Logs.GetResourceLogs()[0]
	assert.Nil(t, rl.GetResource())
	assert.Nil(t, rl.GetScopeLogs()[0].GetScope())
	assert.Equal(t, []*logspb.LogRecord{record}, rl.GetScopeLogs()[0].GetLogRecords())
}

func TestGroupEventsBySubjectsSharesData(t *testing.T) {
	t.Parallel()

	data := otlpgen.Logs(otlpgen.Shape{Records: 4, Resources: 2, Teams: 2})

	t.Run("one subject publishes the payload as it is", func(t *testing.T) {
		t.Parallel()

		messages, err := natssink.GroupEventsBySubjects(newSink(
			t,
			map[string]any{"logs": "'logs.all'"},
		), &domain.Event{Logs: data})
		require.NoError(t, err)
		require.Len(t, messages, 1)
		assert.Same(t, data, messages[0].Event.Logs)
	})

	t.Run("split payloads share records, resources and scopes", func(t *testing.T) {
		t.Parallel()

		messages, err := natssink.GroupEventsBySubjects(
			newSink(t, map[string]any{"logs": logsByTeam}),
			&domain.Event{Logs: data},
		)
		require.NoError(t, err)
		require.Len(t, messages, 2)
		got := messages[0].Event.Logs.GetResourceLogs()[0]
		assert.Same(t, data.GetResourceLogs()[0].GetResource(), got.GetResource())
		assert.Same(t, data.GetResourceLogs()[0].GetScopeLogs()[0].GetScope(), got.GetScopeLogs()[0].GetScope())
		assert.Same(
			t,
			data.GetResourceLogs()[0].GetScopeLogs()[0].GetLogRecords()[0],
			got.GetScopeLogs()[0].GetLogRecords()[0],
		)
	})
}

func TestGroupEventsBySubjectsOrder(t *testing.T) {
	t.Parallel()

	sink := newSink(
		t,
		map[string]any{"logs": "'logs.' + logs.resource_logs[0].resource.attributes.get_attr('service.name')"},
	)
	data := otlpgen.Logs(otlpgen.Shape{Records: 6, Resources: 3, Teams: 1})

	messages, err := natssink.GroupEventsBySubjects(sink, &domain.Event{Logs: data})
	require.NoError(t, err)

	var subjects []string
	for _, m := range messages {
		subjects = append(subjects, m.Subject)
		assert.Equal(t, 2, m.Event.RecordCount())
	}
	assert.Equal(
		t,
		[]string{"logs.service-0", "logs.service-1", "logs.service-2"},
		subjects,
		"subjects in order of first occurrence",
	)
}

func TestGroupEventsBySubjectsKeepsSeparateResourceEntries(t *testing.T) {
	t.Parallel()

	first := otlpgen.Logs(otlpgen.Shape{Records: 2, Resources: 1, Teams: 2})
	second := otlpgen.Logs(otlpgen.Shape{Records: 2, Resources: 1, Teams: 2})
	merged := &logspb.LogsData{ResourceLogs: append(first.ResourceLogs, second.GetResourceLogs()...)}

	messages, err := natssink.GroupEventsBySubjects(
		newSink(t, map[string]any{"logs": logsByTeam}),
		&domain.Event{Logs: merged},
	)
	require.NoError(t, err)
	require.Len(t, messages, 2)
	for _, m := range messages {
		assert.Len(t, m.Event.Logs.GetResourceLogs(), 2)
		assert.Equal(t, 2, m.Event.RecordCount())
	}
}
