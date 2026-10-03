package nats

import (
	"fmt"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/interpreter"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"

	"codeberg.org/eljoth/tram/internal/core/domain"

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
		logs, err := group(n.logs, logsStructure(), event.Logs)
		if err != nil {
			return nil, fmt.Errorf("logs: %w", err)
		}
		messages = append(messages, logs...)
	}

	if event.Metrics != nil && n.Spec().Subject.Metrics != "" {
		metrics, err := group(n.metrics, metricsStructure(), event.Metrics)
		if err != nil {
			return nil, fmt.Errorf("metrics: %w", err)
		}
		messages = append(messages, metrics...)
	}

	if event.Traces != nil && n.Spec().Subject.Traces != "" {
		traces, err := group(n.traces, tracesStructure(), event.Traces)
		if err != nil {
			return nil, fmt.Errorf("traces: %w", err)
		}
		messages = append(messages, traces...)
	}

	return messages, nil
}

// structure describes how a signal type nests its entries: payload D holds
// resources R, which hold scopes S, which hold entries E (log records, metrics
// or spans). The functions build and fill new containers and only copy the
// container fields; entries, resources and scopes are shared.
type structure[D any, R, S, E comparable] struct {
	variable  string
	resources func(D) []R
	scopes    func(R) []S
	entries   func(S) []E
	// payload returns a payload holding entries under a copy of r and s.
	payload        func(r R, s S, entries []E) D
	newPayload     func() D
	newResource    func(R) R
	newScope       func(S) S
	appendResource func(D, R)
	appendScope    func(R, S)
	appendEntry    func(S, E)
	event          func(D) *domain.Event
}

//nolint:dupl // Same shape for each OTLP type; the shared logic is in group.
func logsStructure() structure[*logspb.LogsData, *logspb.ResourceLogs, *logspb.ScopeLogs, *logspb.LogRecord] {
	return structure[*logspb.LogsData, *logspb.ResourceLogs, *logspb.ScopeLogs, *logspb.LogRecord]{
		variable:  logsVariable,
		resources: (*logspb.LogsData).GetResourceLogs,
		scopes:    (*logspb.ResourceLogs).GetScopeLogs,
		entries:   (*logspb.ScopeLogs).GetLogRecords,
		payload: func(r *logspb.ResourceLogs, s *logspb.ScopeLogs, records []*logspb.LogRecord) *logspb.LogsData {
			scope := &logspb.ScopeLogs{Scope: s.GetScope(), SchemaUrl: s.GetSchemaUrl(), LogRecords: records}
			resource := &logspb.ResourceLogs{Resource: r.GetResource(), SchemaUrl: r.GetSchemaUrl()}
			resource.ScopeLogs = []*logspb.ScopeLogs{scope}
			return &logspb.LogsData{ResourceLogs: []*logspb.ResourceLogs{resource}}
		},
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
		event:          func(d *logspb.LogsData) *domain.Event { return &domain.Event{Logs: d} },
	}
}

//nolint:dupl // Same shape for each OTLP type; the shared logic is in group.
func metricsStructure() structure[*metricspb.MetricsData, *metricspb.ResourceMetrics, *metricspb.ScopeMetrics, *metricspb.Metric] {
	return structure[*metricspb.MetricsData, *metricspb.ResourceMetrics, *metricspb.ScopeMetrics, *metricspb.Metric]{
		variable:  metricsVariable,
		resources: (*metricspb.MetricsData).GetResourceMetrics,
		scopes:    (*metricspb.ResourceMetrics).GetScopeMetrics,
		entries:   (*metricspb.ScopeMetrics).GetMetrics,
		payload: func(r *metricspb.ResourceMetrics, s *metricspb.ScopeMetrics, metrics []*metricspb.Metric) *metricspb.MetricsData {
			scope := &metricspb.ScopeMetrics{Scope: s.GetScope(), SchemaUrl: s.GetSchemaUrl(), Metrics: metrics}
			resource := &metricspb.ResourceMetrics{Resource: r.GetResource(), SchemaUrl: r.GetSchemaUrl()}
			resource.ScopeMetrics = []*metricspb.ScopeMetrics{scope}
			return &metricspb.MetricsData{ResourceMetrics: []*metricspb.ResourceMetrics{resource}}
		},
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
		event:       func(d *metricspb.MetricsData) *domain.Event { return &domain.Event{Metrics: d} },
	}
}

//nolint:dupl // Same shape for each OTLP type; the shared logic is in group.
func tracesStructure() structure[*tracespb.TracesData, *tracespb.ResourceSpans, *tracespb.ScopeSpans, *tracespb.Span] {
	return structure[*tracespb.TracesData, *tracespb.ResourceSpans, *tracespb.ScopeSpans, *tracespb.Span]{
		variable:  tracesVariable,
		resources: (*tracespb.TracesData).GetResourceSpans,
		scopes:    (*tracespb.ResourceSpans).GetScopeSpans,
		entries:   (*tracespb.ScopeSpans).GetSpans,
		payload: func(r *tracespb.ResourceSpans, s *tracespb.ScopeSpans, spans []*tracespb.Span) *tracespb.TracesData {
			scope := &tracespb.ScopeSpans{Scope: s.GetScope(), SchemaUrl: s.GetSchemaUrl(), Spans: spans}
			resource := &tracespb.ResourceSpans{Resource: r.GetResource(), SchemaUrl: r.GetSchemaUrl()}
			resource.ScopeSpans = []*tracespb.ScopeSpans{scope}
			return &tracespb.TracesData{ResourceSpans: []*tracespb.ResourceSpans{resource}}
		},
		newPayload: func() *tracespb.TracesData { return &tracespb.TracesData{} },
		newResource: func(r *tracespb.ResourceSpans) *tracespb.ResourceSpans {
			return &tracespb.ResourceSpans{Resource: r.GetResource(), SchemaUrl: r.GetSchemaUrl()}
		},
		newScope: func(s *tracespb.ScopeSpans) *tracespb.ScopeSpans {
			return &tracespb.ScopeSpans{Scope: s.GetScope(), SchemaUrl: s.GetSchemaUrl()}
		},
		appendResource: func(d *tracespb.TracesData, r *tracespb.ResourceSpans) {
			d.ResourceSpans = append(d.ResourceSpans, r)
		},
		appendScope: func(r *tracespb.ResourceSpans, s *tracespb.ScopeSpans) { r.ScopeSpans = append(r.ScopeSpans, s) },
		appendEntry: func(s *tracespb.ScopeSpans, e *tracespb.Span) { s.Spans = append(s.Spans, e) },
		event:       func(d *tracespb.TracesData) *domain.Event { return &domain.Event{Traces: d} },
	}
}

// group evaluates the expression for each entry in two passes: the first
// resolves all subjects, the second groups the entries only if they differ.
// The expression sees a payload containing just the current entry in its
// resource and scope; that payload is built once per scope and reused.
func group[D any, R, S, E comparable](program cel.Program, st structure[D, R, S, E], data D) ([]message, error) {
	subjects, err := resolveSubjects(program, st, data)
	if err != nil || len(subjects) == 0 {
		return nil, err
	}
	if allEqual(subjects) {
		return []message{{subject: subjects[0], event: st.event(data)}}, nil
	}

	var order []string
	groups := map[string]*subjectGroup[D, R, S, E]{}
	i := 0
	for _, r := range st.resources(data) {
		for _, s := range st.scopes(r) {
			for _, e := range st.entries(s) {
				g := groups[subjects[i]]
				if g == nil {
					g = &subjectGroup[D, R, S, E]{structure: st, data: st.newPayload()}
					groups[subjects[i]] = g
					order = append(order, subjects[i])
				}
				g.add(r, s, e)
				i++
			}
		}
	}

	messages := make([]message, len(order))
	for j, subject := range order {
		messages[j] = message{subject: subject, event: st.event(groups[subject].data)}
	}
	return messages, nil
}

func resolveSubjects[D any, R, S, E comparable](
	program cel.Program,
	st structure[D, R, S, E],
	data D,
) ([]string, error) {
	vars := &activation{name: st.variable}
	var subjects []string
	for _, r := range st.resources(data) {
		for _, s := range st.scopes(r) {
			current := make([]E, 1)
			vars.value = st.payload(r, s, current)
			for _, e := range st.entries(s) {
				current[0] = e
				subject, err := evalSubject(program, vars)
				if err != nil {
					return nil, err
				}
				subjects = append(subjects, subject)
			}
		}
	}
	return subjects, nil
}

// subjectGroup collects the entries of one subject. Entries arrive resource
// by resource and scope by scope, so remembering the last input containers is
// enough to find the output containers they belong to.
type subjectGroup[D any, R, S, E comparable] struct {
	structure[D, R, S, E]

	data                D
	resourceIn, scopeIn any
	resource            R
	scope               S
	started             bool
}

func (g *subjectGroup[D, R, S, E]) add(r R, s S, e E) {
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
