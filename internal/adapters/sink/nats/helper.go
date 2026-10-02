package nats

import (
	"fmt"

	"codeberg.org/eljoth/tram/internal/core/domain"
	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/interpreter"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"

	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
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

// message is one NATS message: the records of one signal type that resolved
// to the same subject.
type message struct {
	subject string
	event   *domain.Event
}

// groupEventsBySubjects evaluates the subject expressions for every log
// record, metric and span, and groups them into one message per subject and
// signal type, in the order the subjects first occur.
//
// The event is shared with other branches, so it is not modified: records,
// resources and scopes are shared with it, and only the containers holding
// them are new. Records keep their resource and scope entry; equal resources
// from different entries of the event stay separate entries, which OTLP
// allows. If all records of a signal type resolve to the same subject, its
// payload is published as it is.
func (n *NATS) groupEventsBySubjects(event *domain.Event) ([]message, error) {
	var messages []message

	if event.Logs != nil && n.Spec().Subject.Logs != "" {
		logs, err := groupLogs(n.logs, event.Logs)
		if err != nil {
			return nil, fmt.Errorf("logs: %w", err)
		}
		messages = append(messages, logs...)
	}

	if event.Metrics != nil && n.Spec().Subject.Metrics != "" {
		metrics, err := groupMetrics(n.metrics, event.Metrics)
		if err != nil {
			return nil, fmt.Errorf("metrics: %w", err)
		}
		messages = append(messages, metrics...)
	}

	if event.Traces != nil && n.Spec().Subject.Traces != "" {
		traces, err := groupTraces(n.traces, event.Traces)
		if err != nil {
			return nil, fmt.Errorf("traces: %w", err)
		}
		messages = append(messages, traces...)
	}

	return messages, nil
}

// The group functions evaluate the expression for each record in two passes:
// the first resolves all subjects, the second groups the records only if they
// differ. The expression sees a payload containing just the current record in
// its resource and scope; that payload is built once per scope and reused.

func groupLogs(program cel.Program, data *logspb.LogsData) ([]message, error) {
	vars := &activation{name: "logs"}
	var subjects []string
	for _, rl := range data.ResourceLogs {
		for _, sl := range rl.ScopeLogs {
			current := make([]*logspb.LogRecord, 1)
			vars.value = &logspb.LogsData{ResourceLogs: []*logspb.ResourceLogs{{
				Resource:  rl.Resource,
				SchemaUrl: rl.SchemaUrl,
				ScopeLogs: []*logspb.ScopeLogs{{Scope: sl.Scope, SchemaUrl: sl.SchemaUrl, LogRecords: current}},
			}}}
			for _, record := range sl.LogRecords {
				current[0] = record
				subject, err := evalSubject(program, vars)
				if err != nil {
					return nil, err
				}
				subjects = append(subjects, subject)
			}
		}
	}

	if len(subjects) == 0 {
		return nil, nil
	}
	if allEqual(subjects) {
		return []message{{subject: subjects[0], event: &domain.Event{Logs: data}}}, nil
	}

	var order []string
	groups := map[string]*logsGroup{}
	i := 0
	for _, rl := range data.ResourceLogs {
		for _, sl := range rl.ScopeLogs {
			for _, record := range sl.LogRecords {
				g := groups[subjects[i]]
				if g == nil {
					g = &logsGroup{data: &logspb.LogsData{}}
					groups[subjects[i]] = g
					order = append(order, subjects[i])
				}
				g.add(rl, sl, record)
				i++
			}
		}
	}

	messages := make([]message, len(order))
	for j, subject := range order {
		messages[j] = message{subject: subject, event: &domain.Event{Logs: groups[subject].data}}
	}
	return messages, nil
}

// logsGroup collects the records of one subject. Records arrive resource by
// resource and scope by scope, so remembering the last input entries is
// enough to find the output entries they belong to.
type logsGroup struct {
	data                *logspb.LogsData
	resourceIn, scopeIn any
	resource            *logspb.ResourceLogs
	scope               *logspb.ScopeLogs
}

func (g *logsGroup) add(rl *logspb.ResourceLogs, sl *logspb.ScopeLogs, record *logspb.LogRecord) {
	if g.resourceIn != any(rl) {
		g.resource = &logspb.ResourceLogs{Resource: rl.Resource, SchemaUrl: rl.SchemaUrl}
		g.data.ResourceLogs = append(g.data.ResourceLogs, g.resource)
		g.resourceIn, g.scopeIn = rl, nil
	}
	if g.scopeIn != any(sl) {
		g.scope = &logspb.ScopeLogs{Scope: sl.Scope, SchemaUrl: sl.SchemaUrl}
		g.resource.ScopeLogs = append(g.resource.ScopeLogs, g.scope)
		g.scopeIn = sl
	}
	g.scope.LogRecords = append(g.scope.LogRecords, record)
}

func groupMetrics(program cel.Program, data *metricspb.MetricsData) ([]message, error) {
	vars := &activation{name: "metrics"}
	var subjects []string
	for _, rm := range data.ResourceMetrics {
		for _, sm := range rm.ScopeMetrics {
			current := make([]*metricspb.Metric, 1)
			vars.value = &metricspb.MetricsData{ResourceMetrics: []*metricspb.ResourceMetrics{{
				Resource:     rm.Resource,
				SchemaUrl:    rm.SchemaUrl,
				ScopeMetrics: []*metricspb.ScopeMetrics{{Scope: sm.Scope, SchemaUrl: sm.SchemaUrl, Metrics: current}},
			}}}
			for _, metric := range sm.Metrics {
				current[0] = metric
				subject, err := evalSubject(program, vars)
				if err != nil {
					return nil, err
				}
				subjects = append(subjects, subject)
			}
		}
	}

	if len(subjects) == 0 {
		return nil, nil
	}
	if allEqual(subjects) {
		return []message{{subject: subjects[0], event: &domain.Event{Metrics: data}}}, nil
	}

	var order []string
	groups := map[string]*metricsGroup{}
	i := 0
	for _, rm := range data.ResourceMetrics {
		for _, sm := range rm.ScopeMetrics {
			for _, metric := range sm.Metrics {
				g := groups[subjects[i]]
				if g == nil {
					g = &metricsGroup{data: &metricspb.MetricsData{}}
					groups[subjects[i]] = g
					order = append(order, subjects[i])
				}
				g.add(rm, sm, metric)
				i++
			}
		}
	}

	messages := make([]message, len(order))
	for j, subject := range order {
		messages[j] = message{subject: subject, event: &domain.Event{Metrics: groups[subject].data}}
	}
	return messages, nil
}

// metricsGroup collects the metrics of one subject; see logsGroup.
type metricsGroup struct {
	data                *metricspb.MetricsData
	resourceIn, scopeIn any
	resource            *metricspb.ResourceMetrics
	scope               *metricspb.ScopeMetrics
}

func (g *metricsGroup) add(rm *metricspb.ResourceMetrics, sm *metricspb.ScopeMetrics, metric *metricspb.Metric) {
	if g.resourceIn != any(rm) {
		g.resource = &metricspb.ResourceMetrics{Resource: rm.Resource, SchemaUrl: rm.SchemaUrl}
		g.data.ResourceMetrics = append(g.data.ResourceMetrics, g.resource)
		g.resourceIn, g.scopeIn = rm, nil
	}
	if g.scopeIn != any(sm) {
		g.scope = &metricspb.ScopeMetrics{Scope: sm.Scope, SchemaUrl: sm.SchemaUrl}
		g.resource.ScopeMetrics = append(g.resource.ScopeMetrics, g.scope)
		g.scopeIn = sm
	}
	g.scope.Metrics = append(g.scope.Metrics, metric)
}

func groupTraces(program cel.Program, data *tracespb.TracesData) ([]message, error) {
	vars := &activation{name: "traces"}
	var subjects []string
	for _, rs := range data.ResourceSpans {
		for _, ss := range rs.ScopeSpans {
			current := make([]*tracespb.Span, 1)
			vars.value = &tracespb.TracesData{ResourceSpans: []*tracespb.ResourceSpans{{
				Resource:   rs.Resource,
				SchemaUrl:  rs.SchemaUrl,
				ScopeSpans: []*tracespb.ScopeSpans{{Scope: ss.Scope, SchemaUrl: ss.SchemaUrl, Spans: current}},
			}}}
			for _, span := range ss.Spans {
				current[0] = span
				subject, err := evalSubject(program, vars)
				if err != nil {
					return nil, err
				}
				subjects = append(subjects, subject)
			}
		}
	}

	if len(subjects) == 0 {
		return nil, nil
	}
	if allEqual(subjects) {
		return []message{{subject: subjects[0], event: &domain.Event{Traces: data}}}, nil
	}

	var order []string
	groups := map[string]*tracesGroup{}
	i := 0
	for _, rs := range data.ResourceSpans {
		for _, ss := range rs.ScopeSpans {
			for _, span := range ss.Spans {
				g := groups[subjects[i]]
				if g == nil {
					g = &tracesGroup{data: &tracespb.TracesData{}}
					groups[subjects[i]] = g
					order = append(order, subjects[i])
				}
				g.add(rs, ss, span)
				i++
			}
		}
	}

	messages := make([]message, len(order))
	for j, subject := range order {
		messages[j] = message{subject: subject, event: &domain.Event{Traces: groups[subject].data}}
	}
	return messages, nil
}

// tracesGroup collects the spans of one subject; see logsGroup.
type tracesGroup struct {
	data                *tracespb.TracesData
	resourceIn, scopeIn any
	resource            *tracespb.ResourceSpans
	scope               *tracespb.ScopeSpans
}

func (g *tracesGroup) add(rs *tracespb.ResourceSpans, ss *tracespb.ScopeSpans, span *tracespb.Span) {
	if g.resourceIn != any(rs) {
		g.resource = &tracespb.ResourceSpans{Resource: rs.Resource, SchemaUrl: rs.SchemaUrl}
		g.data.ResourceSpans = append(g.data.ResourceSpans, g.resource)
		g.resourceIn, g.scopeIn = rs, nil
	}
	if g.scopeIn != any(ss) {
		g.scope = &tracespb.ScopeSpans{Scope: ss.Scope, SchemaUrl: ss.SchemaUrl}
		g.resource.ScopeSpans = append(g.resource.ScopeSpans, g.scope)
		g.scopeIn = ss
	}
	g.scope.Spans = append(g.scope.Spans, span)
}

func evalSubject(program cel.Program, vars *activation) (string, error) {
	out, _, err := program.Eval(vars)
	if err != nil {
		return "", fmt.Errorf("failed to evaluate CEL: %w", err)
	}
	subject, ok := out.Value().(string)
	if !ok {
		return "", fmt.Errorf("CEL expression returned %T instead of a string", out.Value())
	}
	return subject, nil
}

func allEqual(subjects []string) bool {
	for _, s := range subjects[1:] {
		if s != subjects[0] {
			return false
		}
	}
	return true
}

// activation provides a single variable to CEL. It is reused for all records
// of a payload to avoid an allocation per evaluation.
type activation struct {
	name  string
	value any
}

func (a *activation) ResolveName(name string) (any, bool) {
	if name == a.name {
		return a.value, true
	}
	return nil, false
}

func (a *activation) Parent() interpreter.Activation {
	return nil
}
