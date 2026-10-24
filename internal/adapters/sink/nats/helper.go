// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package nats

import (
	"fmt"
	"strings"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/interpreter"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	"google.golang.org/protobuf/proto"

	"codeberg.org/eljoth/tram/internal/adapters/natspartition"
	"codeberg.org/eljoth/tram/internal/core/domain"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracespb "go.opentelemetry.io/proto/otlp/trace/v1"
)

func initCEL(celEnv *cel.Env, expression string, opts ...cel.EnvOption) (cel.Program, error) {
	env, err := celEnv.Extend(opts...)
	if err != nil {
		return nil, err
	}
	if expression == "" {
		expression = "true"
	}
	ast, issues := env.Compile(expression)
	if issues != nil && issues.Err() != nil {
		return nil, fmt.Errorf("failed to compile CEL expression: %w", issues.Err())
	}

	celProgram, err := env.Program(ast)
	if err != nil {
		return nil, fmt.Errorf("failed to build CEL program: %w", err)
	}
	return celProgram, nil
}

type message struct {
	subject string
	event   *domain.Event
}

func (n *NATS) groupEventsBySubjects(event *domain.Event) ([]message, error) {
	var messages []message
	var keys subjects
	if n.partitions != nil {
		keys = n.partitions.keys
	}

	if event.Logs != nil && n.Spec().Subject.Logs != "" {
		parts, err := domain.PartitionLogs(event.Logs, logSubject(n.logs, n.partitioner(keys.logs)))
		if err != nil {
			return nil, fmt.Errorf("logs: %w", err)
		}
		messages = appendMessages(messages, parts, func(d *logspb.LogsData) *domain.Event {
			return &domain.Event{Logs: d}
		})
	}

	if event.Metrics != nil && n.Spec().Subject.Metrics != "" {
		parts, err := domain.PartitionMetrics(event.Metrics, metricSubject(n.metrics, n.partitioner(keys.metrics)))
		if err != nil {
			return nil, fmt.Errorf("metrics: %w", err)
		}
		messages = appendMessages(messages, parts, func(d *metricspb.MetricsData) *domain.Event {
			return &domain.Event{Metrics: d}
		})
	}

	if event.Traces != nil && n.Spec().Subject.Traces != "" {
		parts, err := domain.PartitionSpans(event.Traces, spanSubject(n.traces, n.partitioner(keys.traces)))
		if err != nil {
			return nil, fmt.Errorf("traces: %w", err)
		}
		messages = appendMessages(messages, parts, func(d *tracespb.TracesData) *domain.Event {
			return &domain.Event{Traces: d}
		})
	}

	return messages, nil
}

func appendMessages[D any](messages []message, parts []domain.Part[D], event func(D) *domain.Event) []message {
	for _, part := range parts {
		messages = append(messages, message{subject: part.Key, event: event(part.Data)})
	}
	return messages
}

// The ...Subject functions evaluate a subject expression per entry, with the
// variables set to the entry, its resource and scope, and a payload that holds
// only the entry. The payload is built once per scope.

//nolint:dupl // Same shape as spanSubject, for another signal.
func logSubject(program cel.Program, part *partitioner) domain.LogKey {
	vars := &activation{name: logsVariable}
	var resourceIn *logspb.ResourceLogs
	var scopeIn *logspb.ScopeLogs
	current := make([]*logspb.LogRecord, 1)
	return func(r *logspb.ResourceLogs, s *logspb.ScopeLogs, record *logspb.LogRecord) (string, error) {
		if r != resourceIn || s != scopeIn {
			resourceIn, scopeIn = r, s
			vars.setContainers(r.GetResource(), s.GetScope())
			vars.value = &logspb.LogsData{ResourceLogs: []*logspb.ResourceLogs{{
				Resource:  r.GetResource(),
				SchemaUrl: r.GetSchemaUrl(),
				ScopeLogs: []*logspb.ScopeLogs{{Scope: s.GetScope(), SchemaUrl: s.GetSchemaUrl(), LogRecords: current}},
			}}}
		}
		current[0] = record
		vars.record = record
		return resolveSubject(program, part, vars)
	}
}

//nolint:dupl // Same shape as logSubject, for another signal.
func spanSubject(program cel.Program, part *partitioner) domain.SpanKey {
	vars := &activation{name: tracesVariable}
	var resourceIn *tracespb.ResourceSpans
	var scopeIn *tracespb.ScopeSpans
	current := make([]*tracespb.Span, 1)
	return func(r *tracespb.ResourceSpans, s *tracespb.ScopeSpans, span *tracespb.Span) (string, error) {
		if r != resourceIn || s != scopeIn {
			resourceIn, scopeIn = r, s
			vars.setContainers(r.GetResource(), s.GetScope())
			vars.value = &tracespb.TracesData{ResourceSpans: []*tracespb.ResourceSpans{{
				Resource:   r.GetResource(),
				SchemaUrl:  r.GetSchemaUrl(),
				ScopeSpans: []*tracespb.ScopeSpans{{Scope: s.GetScope(), SchemaUrl: s.GetSchemaUrl(), Spans: current}},
			}}}
		}
		current[0] = span
		vars.span = span
		return resolveSubject(program, part, vars)
	}
}

// metricSubject evaluates per data point. The payload holds the whole metric.
func metricSubject(program cel.Program, part *partitioner) domain.MetricKey {
	vars := &activation{name: metricsVariable}
	var resourceIn *metricspb.ResourceMetrics
	var scopeIn *metricspb.ScopeMetrics
	current := make([]*metricspb.Metric, 1)
	return func(
		r *metricspb.ResourceMetrics,
		s *metricspb.ScopeMetrics,
		m *metricspb.Metric,
		dataPoint proto.Message,
	) (string, error) {
		if r != resourceIn || s != scopeIn {
			resourceIn, scopeIn = r, s
			vars.setContainers(r.GetResource(), s.GetScope())
			vars.value = &metricspb.MetricsData{ResourceMetrics: []*metricspb.ResourceMetrics{
				{
					Resource:  r.GetResource(),
					SchemaUrl: r.GetSchemaUrl(),
					ScopeMetrics: []*metricspb.ScopeMetrics{
						{Scope: s.GetScope(), SchemaUrl: s.GetSchemaUrl(), Metrics: current},
					},
				},
			}}
		}
		current[0] = m
		vars.metric = m
		vars.dataPoint = dataPoint
		return resolveSubject(program, part, vars)
	}
}

// resolveSubject evaluates the subject of the entry in vars. With part, it
// first sets the entry's partition and then checks that the subject belongs
// to it.
func resolveSubject(program cel.Program, part *partitioner, vars *activation) (string, error) {
	if part == nil {
		return evalSubject(program, vars)
	}
	key, err := evalString(part.key, vars)
	if err != nil {
		return "", fmt.Errorf("partition key: %w", err)
	}
	partition := natspartition.Of(key, part.count)
	vars.partition = int64(partition)
	subject, err := evalSubject(program, vars)
	if err != nil {
		return "", err
	}
	return subject, part.check(subject, partition)
}

func evalSubject(program cel.Program, vars *activation) (string, error) {
	subject, err := evalString(program, vars)
	if err != nil {
		return "", err
	}
	if err = validSubject(subject); err != nil {
		return "", err
	}
	return subject, nil
}

func evalString(program cel.Program, vars *activation) (string, error) {
	out, _, err := program.Eval(vars)
	if err != nil {
		return "", fmt.Errorf("failed to evaluate CEL: %w", err)
	}
	value, ok := out.Value().(string)
	if !ok {
		return "", fmt.Errorf("CEL expression returned %T instead of a string", out.Value())
	}
	return value, nil
}

// partitioner checks the subjects of one signal of an event against the
// partitions of their entries.
type partitioner struct {
	*partitions

	key cel.Program
	// checked holds the partition each subject was found in, so a subject is
	// checked once per event.
	checked map[string]int
}

// partitioner returns nil without partitioning.
func (n *NATS) partitioner(key cel.Program) *partitioner {
	if n.partitions == nil {
		return nil
	}
	return &partitioner{partitions: n.partitions, key: key, checked: map[string]int{}}
}

// check returns an error if subject is not stored in the stream of partition.
// JetStream picks the stream by subject, so a subject expression that ignores
// the partition would otherwise put entries into another partition's stream.
func (p *partitioner) check(subject string, partition int) error {
	if checked, ok := p.checked[subject]; ok && checked == partition {
		return nil
	}
	if !covered(p.registers[partition], []string{subject}) {
		return fmt.Errorf("subject %q is not stored in partition %d, whose stream holds %v",
			subject, partition, p.registers[partition])
	}
	p.checked[subject] = partition
	return nil
}

func validSubject(subject string) error {
	for token := range strings.SplitSeq(subject, ".") {
		switch {
		case token == "":
			return fmt.Errorf("subject %q has an empty token", subject)
		case token == "*" || token == ">":
			return fmt.Errorf("subject %q contains a wildcard", subject)
		case strings.ContainsAny(token, " \t\r\n"):
			return fmt.Errorf("subject %q contains whitespace", subject)
		}
	}
	return nil
}

type activation struct {
	name      string
	value     any
	resource  any
	scope     any
	record    any
	metric    any
	dataPoint proto.Message
	span      any
	partition int64

	emptyResource *resourcepb.Resource
	emptyScope    *commonpb.InstrumentationScope
}

// setContainers sets the resource and scope variables. A missing resource or
// scope is an empty message, so expressions can read its fields without checks.
func (a *activation) setContainers(resource *resourcepb.Resource, scope *commonpb.InstrumentationScope) {
	if resource == nil {
		if a.emptyResource == nil {
			a.emptyResource = &resourcepb.Resource{}
		}
		resource = a.emptyResource
	}
	if scope == nil {
		if a.emptyScope == nil {
			a.emptyScope = &commonpb.InstrumentationScope{}
		}
		scope = a.emptyScope
	}
	a.resource, a.scope = resource, scope
}

func (a *activation) ResolveName(name string) (any, bool) {
	switch name {
	case a.name:
		return a.value, true
	case resourceVariable:
		return a.resource, true
	case scopeVariable:
		return a.scope, true

	case recordVariable:
		return a.record, true
	case metricVariable:
		return a.metric, true
	case dataPointVariable:
		// Absent for a metric without data points, so reading it fails.
		return a.dataPoint, a.dataPoint != nil
	case spanVariable:
		return a.span, true
	case partitionVariable:
		return a.partition, true
	}
	return nil, false
}

func (a *activation) Parent() interpreter.Activation {
	return nil
}
