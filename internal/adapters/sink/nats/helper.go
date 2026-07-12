package nats

import (
	"fmt"

	"codeberg.org/eljoth/tram/internal/core/domain"
	"github.com/google/cel-go/cel"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/protobuf/proto"

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

func (n *NATS) groupEventsBySubjects(event *domain.Event) (map[string]*domain.Event, error) {
	result := make(map[string]*domain.Event)

	if event.Logs != nil && n.Spec().Subject.Logs != "" {
		for _, resLogs := range event.Logs.ResourceLogs {
			for _, scopeLogs := range resLogs.ScopeLogs {
				for _, record := range scopeLogs.LogRecords {

					singleRecordPayload := &logspb.LogsData{
						ResourceLogs: []*logspb.ResourceLogs{
							{
								Resource: resLogs.Resource,
								ScopeLogs: []*logspb.ScopeLogs{
									{
										Scope:      scopeLogs.Scope,
										LogRecords: []*logspb.LogRecord{record},
									},
								},
							},
						},
					}
					out, _, err := n.logs.Eval(map[string]any{
						"logs": singleRecordPayload,
					})
					if err != nil {
						return nil, fmt.Errorf("failed to evaluate logs CEL: %w", err)
					}

					subjectStr, ok := out.Value().(string)
					if !ok {
						return nil, fmt.Errorf("logs CEL expression did not return a string")
					}

					if _, exists := result[subjectStr]; !exists {
						result[subjectStr] = &domain.Event{
							Logs: &logspb.LogsData{},
						}
					}

					appendLogRecord(result[subjectStr].Logs, resLogs, scopeLogs, record)
				}
			}
		}
	}

	if event.Metrics != nil && n.Spec().Subject.Metrics != "" {
		for _, resMetrics := range event.Metrics.ResourceMetrics {
			for _, scopeMetrics := range resMetrics.ScopeMetrics {
				for _, metric := range scopeMetrics.Metrics {
					singlePayload := &metricspb.MetricsData{
						ResourceMetrics: []*metricspb.ResourceMetrics{{
							Resource:     resMetrics.Resource,
							ScopeMetrics: []*metricspb.ScopeMetrics{{Scope: scopeMetrics.Scope, Metrics: []*metricspb.Metric{metric}}},
						}},
					}
					out, _, err := n.metrics.Eval(map[string]any{"metrics": singlePayload})
					if err != nil {
						return nil, fmt.Errorf("failed to evaluate metrics CEL: %w", err)
					}
					subjectStr, ok := out.Value().(string)
					if !ok {
						return nil, fmt.Errorf("metrics CEL expression did not return a string")
					}

					if _, exists := result[subjectStr]; !exists {
						result[subjectStr] = &domain.Event{Metrics: &metricspb.MetricsData{}}
					}
					appendMetric(result[subjectStr].Metrics, resMetrics, scopeMetrics, metric)
				}
			}
		}
	}

	if event.Traces != nil && n.Spec().Subject.Traces != "" {
		for _, resSpans := range event.Traces.ResourceSpans {
			for _, scopeSpans := range resSpans.ScopeSpans {
				for _, span := range scopeSpans.Spans {
					singlePayload := &tracespb.TracesData{
						ResourceSpans: []*tracespb.ResourceSpans{{
							Resource:   resSpans.Resource,
							ScopeSpans: []*tracespb.ScopeSpans{{Scope: scopeSpans.Scope, Spans: []*tracespb.Span{span}}},
						}},
					}
					out, _, err := n.traces.Eval(map[string]any{"traces": singlePayload})
					if err != nil {
						return nil, fmt.Errorf("failed to evaluate traces CEL: %w", err)
					}
					subjectStr, ok := out.Value().(string)
					if !ok {
						return nil, fmt.Errorf("traces CEL expression did not return a string")
					}

					if _, exists := result[subjectStr]; !exists {
						result[subjectStr] = &domain.Event{Traces: &tracespb.TracesData{}}
					}
					appendSpan(result[subjectStr].Traces, resSpans, scopeSpans, span)
				}
			}
		}
	}

	return result, nil
}

func appendLogRecord(target *logspb.LogsData, origRes *logspb.ResourceLogs, origScope *logspb.ScopeLogs, record *logspb.LogRecord) {
	var targetRes *logspb.ResourceLogs
	for _, rl := range target.ResourceLogs {
		if proto.Equal(rl.Resource, origRes.Resource) {
			targetRes = rl
			break
		}
	}

	if targetRes == nil {
		targetRes = &logspb.ResourceLogs{
			Resource:  proto.Clone(origRes.Resource).(*resourcepb.Resource),
			ScopeLogs: []*logspb.ScopeLogs{},
		}
		target.ResourceLogs = append(target.ResourceLogs, targetRes)
	}

	var targetScope *logspb.ScopeLogs
	for _, sl := range targetRes.ScopeLogs {
		if proto.Equal(sl.Scope, origScope.Scope) {
			targetScope = sl
			break
		}
	}

	if targetScope == nil {
		targetScope = &logspb.ScopeLogs{
			Scope:      proto.Clone(origScope.Scope).(*commonpb.InstrumentationScope),
			LogRecords: []*logspb.LogRecord{},
		}
		targetRes.ScopeLogs = append(targetRes.ScopeLogs, targetScope)
	}

	targetScope.LogRecords = append(targetScope.LogRecords, proto.Clone(record).(*logspb.LogRecord))
}

func appendMetric(target *metricspb.MetricsData, origRes *metricspb.ResourceMetrics, origScope *metricspb.ScopeMetrics, metric *metricspb.Metric) {
	var targetRes *metricspb.ResourceMetrics
	for _, rm := range target.ResourceMetrics {
		if proto.Equal(rm.Resource, origRes.Resource) {
			targetRes = rm
			break
		}
	}
	if targetRes == nil {
		targetRes = &metricspb.ResourceMetrics{
			Resource:     proto.Clone(origRes.Resource).(*resourcepb.Resource),
			ScopeMetrics: []*metricspb.ScopeMetrics{},
		}
		target.ResourceMetrics = append(target.ResourceMetrics, targetRes)
	}

	var targetScope *metricspb.ScopeMetrics
	for _, sm := range targetRes.ScopeMetrics {
		if proto.Equal(sm.Scope, origScope.Scope) {
			targetScope = sm
			break
		}
	}
	if targetScope == nil {
		targetScope = &metricspb.ScopeMetrics{
			Scope:   proto.Clone(origScope.Scope).(*commonpb.InstrumentationScope),
			Metrics: []*metricspb.Metric{},
		}
		targetRes.ScopeMetrics = append(targetRes.ScopeMetrics, targetScope)
	}

	targetScope.Metrics = append(targetScope.Metrics, proto.Clone(metric).(*metricspb.Metric))
}

func appendSpan(target *tracespb.TracesData, origRes *tracespb.ResourceSpans, origScope *tracespb.ScopeSpans, span *tracespb.Span) {
	var targetRes *tracespb.ResourceSpans
	for _, rs := range target.ResourceSpans {
		if proto.Equal(rs.Resource, origRes.Resource) {
			targetRes = rs
			break
		}
	}
	if targetRes == nil {
		targetRes = &tracespb.ResourceSpans{
			Resource:   proto.Clone(origRes.Resource).(*resourcepb.Resource),
			ScopeSpans: []*tracespb.ScopeSpans{},
		}
		target.ResourceSpans = append(target.ResourceSpans, targetRes)
	}

	var targetScope *tracespb.ScopeSpans
	for _, ss := range targetRes.ScopeSpans {
		if proto.Equal(ss.Scope, origScope.Scope) {
			targetScope = ss
			break
		}
	}
	if targetScope == nil {
		targetScope = &tracespb.ScopeSpans{
			Scope: proto.Clone(origScope.Scope).(*commonpb.InstrumentationScope),
			Spans: []*tracespb.Span{},
		}
		targetRes.ScopeSpans = append(targetRes.ScopeSpans, targetScope)
	}

	targetScope.Spans = append(targetScope.Spans, proto.Clone(span).(*tracespb.Span))
}
