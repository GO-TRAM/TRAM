package nats

import (
	"fmt"
	"strings"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/interpreter"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	"google.golang.org/protobuf/proto"

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

	if event.Logs != nil && n.Spec().Subject.Logs != "" {
		logs, err := group(n.logs, logsStructure(), event.Logs)
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
		traces, err := group(n.traces, tracesStructure(), event.Traces)
		if err != nil {
			return nil, fmt.Errorf("traces: %w", err)
		}
		messages = append(messages, traces...)
	}

	return messages, nil
}

type structure[D any, R, S, E comparable] struct {
	variable       string
	resources      func(D) []R
	scopes         func(R) []S
	entries        func(S) []E
	resource       func(R) *resourcepb.Resource
	scope          func(S) *commonpb.InstrumentationScope
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
		resource:  (*logspb.ResourceLogs).GetResource,
		scope:     (*logspb.ScopeLogs).GetScope,
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
		resource:  (*metricspb.ResourceMetrics).GetResource,
		scope:     (*metricspb.ScopeMetrics).GetScope,
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
		resource:  (*tracespb.ResourceSpans).GetResource,
		scope:     (*tracespb.ScopeSpans).GetScope,
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

func group[D any, R, S, E comparable](program cel.Program, st structure[D, R, S, E], data D) ([]message, error) {
	subjects, err := resolveSubjects(program, st, data)
	if err != nil {
		return nil, err
	}
	return groupBySubjects(st, data, subjects), nil
}

// groupMetrics resolves a subject per data point. A metric whose data points
// resolve to different subjects is split into one metric per subject before
// grouping.
func groupMetrics(program cel.Program, data *metricspb.MetricsData) ([]message, error) {
	parts, subjects, err := splitMetrics(program, data)
	if err != nil {
		return nil, err
	}
	return groupBySubjects(metricsStructure(), parts, subjects), nil
}

// groupBySubjects groups the entries of data, in order, by subjects, which
// holds one subject per entry.
func groupBySubjects[D any, R, S, E comparable](st structure[D, R, S, E], data D, subjects []string) []message {
	if len(subjects) == 0 {
		return nil
	}
	if allEqual(subjects) {
		return []message{{subject: subjects[0], event: st.event(data)}}
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
	return messages
}

// splitMetrics resolves the subject of every data point and returns the
// metrics with one subject each: data itself if no metric has data points
// with different subjects, or else a copy of its structure in which such
// metrics are split. subjects holds one subject per metric of the result.
func splitMetrics(program cel.Program, data *metricspb.MetricsData) (*metricspb.MetricsData, []string, error) {
	st := metricsStructure()
	vars := &activation{name: st.variable}
	subject := func(dataPoint proto.Message) (string, error) {
		vars.dataPoint = dataPoint
		return evalSubject(program, vars)
	}

	var parts []domain.MetricPart
	var counts []int // parts per metric
	err := walk(st, data, vars, func(m *metricspb.Metric) error {
		before := len(parts)
		var err error
		parts, err = domain.AppendMetricParts(parts, m, subject)
		counts = append(counts, len(parts)-before)
		return err
	})
	if err != nil {
		return nil, nil, err
	}

	subjects := make([]string, len(parts))
	for i, part := range parts {
		subjects[i] = part.Key
	}
	if len(parts) == len(counts) {
		// Every metric is one part, so none was split.
		return data, subjects, nil
	}
	return rebuildMetrics(st, data, parts, counts), subjects, nil
}

// rebuildMetrics copies the structure of data, with the metrics replaced by
// their parts. counts holds the number of parts of each metric.
func rebuildMetrics(
	st structure[*metricspb.MetricsData, *metricspb.ResourceMetrics, *metricspb.ScopeMetrics, *metricspb.Metric],
	data *metricspb.MetricsData,
	parts []domain.MetricPart,
	counts []int,
) *metricspb.MetricsData {
	out := st.newPayload()
	metric, part := 0, 0
	for _, rm := range data.GetResourceMetrics() {
		resource := st.newResource(rm)
		st.appendResource(out, resource)
		for _, sm := range rm.GetScopeMetrics() {
			scope := st.newScope(sm)
			st.appendScope(resource, scope)
			for range sm.GetMetrics() {
				for range counts[metric] {
					st.appendEntry(scope, parts[part].Metric)
					part++
				}
				metric++
			}
		}
	}
	return out
}

func resolveSubjects[D any, R, S, E comparable](
	program cel.Program,
	st structure[D, R, S, E],
	data D,
) ([]string, error) {
	vars := &activation{name: st.variable}
	var subjects []string
	err := walk(st, data, vars, func(E) error {
		subject, err := evalSubject(program, vars)
		subjects = append(subjects, subject)
		return err
	})
	if err != nil {
		return nil, err
	}
	return subjects, nil
}

// walk calls visit for every entry of data, with vars set to the entry, its
// resource and scope, and a payload holding only the entry.
func walk[D any, R, S, E comparable](st structure[D, R, S, E], data D, vars *activation, visit func(E) error) error {
	emptyResource, emptyScope := &resourcepb.Resource{}, &commonpb.InstrumentationScope{}
	for _, r := range st.resources(data) {
		resource := st.resource(r)
		if resource == nil {
			resource = emptyResource
		}
		vars.resource = resource
		for _, s := range st.scopes(r) {
			current := make([]E, 1)
			vars.value = st.payload(r, s, current)
			scope := st.scope(s)
			if scope == nil {
				scope = emptyScope
			}
			vars.scope = scope
			for _, e := range st.entries(s) {
				current[0] = e
				vars.entry = e
				switch st.variable {
				case logsVariable:
					vars.record = e
				case metricsVariable:
					vars.metric = e
				case tracesVariable:
					vars.span = e
				}
				if err := visit(e); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

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
	if err = validSubject(subject); err != nil {
		return "", err
	}
	return subject, nil
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

func allEqual(subjects []string) bool {
	for _, s := range subjects[1:] {
		if s != subjects[0] {
			return false
		}
	}
	return true
}

type activation struct {
	name      string
	value     any
	resource  any
	scope     any
	entry     any
	record    any
	metric    any
	dataPoint proto.Message
	span      any
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
	}
	return nil, false
}

func (a *activation) Parent() interpreter.Activation {
	return nil
}
