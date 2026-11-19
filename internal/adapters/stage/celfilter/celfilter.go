// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package celfilter

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/go-playground/validator/v10"
	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/interpreter"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"

	"codeberg.org/eljoth/tram/internal/adapters/stage/base"
	"codeberg.org/eljoth/tram/internal/core/domain"
)

const Type = "cel_filter"

type CELFilter struct {
	*base.Stage[Spec]

	logs    cel.Program
	metrics cel.Program
	traces  cel.Program
	logger  *slog.Logger
}

func New(stage domain.StageSpec, validator *validator.Validate, logger *slog.Logger) (*CELFilter, error) {
	base, err := base.New[Spec](stage.Name, stage.From, stage.Spec, validator)
	if err != nil {
		return nil, err
	}

	env, err := cel.NewEnv(
		cel.TypeDescs(
			commonpb.File_opentelemetry_proto_common_v1_common_proto,
			resourcepb.File_opentelemetry_proto_resource_v1_resource_proto,
			logspb.File_opentelemetry_proto_logs_v1_logs_proto,
			metricspb.File_opentelemetry_proto_metrics_v1_metrics_proto,
			tracepb.File_opentelemetry_proto_trace_v1_trace_proto,
		),
		cel.Variable("resource", cel.ObjectType("opentelemetry.proto.resource.v1.Resource")),
		cel.Variable("scope", cel.ObjectType("opentelemetry.proto.common.v1.InstrumentationScope")),
	)
	if err != nil {
		return nil, errors.Join(ErrCELEnvironmentInitialization, err)
	}

	var logs, metrics, traces cel.Program
	if expression := base.Spec().Logs; expression != "" {
		logs, err = compile(env, expression,
			cel.Variable("record", cel.ObjectType("opentelemetry.proto.logs.v1.LogRecord")))
		if err != nil {
			return nil, errors.Join(ErrCELInvalidLogsExpression, err)
		}
	}

	if expression := base.Spec().Metrics; expression != "" {
		metrics, err = compile(env, expression,
			cel.Variable("metric", cel.ObjectType("opentelemetry.proto.metrics.v1.Metric")),
			cel.Variable("datapoint", cel.DynType))
		if err != nil {
			return nil, errors.Join(ErrCELInvalidMetricsExpression, err)
		}
	}

	if expression := base.Spec().Traces; expression != "" {
		traces, err = compile(env, expression,
			cel.Variable("span", cel.ObjectType("opentelemetry.proto.trace.v1.Span")))
		if err != nil {
			return nil, errors.Join(ErrCELInvalidTracesExpression, err)
		}
	}

	return &CELFilter{
		Stage:   base,
		logs:    logs,
		metrics: metrics,
		traces:  traces,
		logger:  logger.With(slog.String("stage", stage.Name)),
	}, nil
}

func compile(env *cel.Env, expression string, vars ...cel.EnvOption) (cel.Program, error) {
	env, err := env.Extend(vars...)
	if err != nil {
		return nil, err
	}
	ast, issues := env.Compile(expression)
	if issues != nil && issues.Err() != nil {
		return nil, fmt.Errorf("failed to compile CEL expression: %w", issues.Err())
	}
	if t := ast.OutputType(); t != cel.BoolType && t != cel.DynType {
		return nil, fmt.Errorf("CEL expression must return a bool, not %s", t)
	}
	return env.Program(ast)
}

func (f *CELFilter) Notify(ctx context.Context, event *domain.Event) error {
	filtered := *event
	vars := &activation{emptyResource: &resourcepb.Resource{}, emptyScope: &commonpb.InstrumentationScope{}}

	if f.logs != nil && event.Logs != nil {
		logs, err := domain.FilterLogs(
			event.Logs,
			func(resource *resourcepb.Resource, scope *commonpb.InstrumentationScope, record *logspb.LogRecord) (bool, error) {
				vars.set(resource, scope)
				vars.record = record
				return eval(f.logs, vars)
			},
		)
		if err != nil {
			return err
		}
		filtered.Logs = logs
	}

	if f.metrics != nil && event.Metrics != nil {
		metrics, err := domain.FilterMetrics(
			event.Metrics,
			func(resource *resourcepb.Resource, scope *commonpb.InstrumentationScope, metric *metricspb.Metric, dataPoint proto.Message) (bool, error) {
				vars.set(resource, scope)
				vars.metric = metric
				vars.dataPoint = dataPoint
				return eval(f.metrics, vars)
			},
		)
		if err != nil {
			return err
		}
		filtered.Metrics = metrics
	}

	if f.traces != nil && event.Traces != nil {
		traces, err := domain.FilterSpans(
			event.Traces,
			func(resource *resourcepb.Resource, scope *commonpb.InstrumentationScope, span *tracepb.Span) (bool, error) {
				vars.set(resource, scope)
				vars.span = span
				return eval(f.traces, vars)
			},
		)
		if err != nil {
			return err
		}
		filtered.Traces = traces
	}

	if filtered == *event {
		return f.NotifySubscribers(ctx, event)
	}
	if filtered.RecordCount() == 0 {
		return nil
	}
	return f.NotifySubscribers(ctx, &filtered)
}

func eval(program cel.Program, vars *activation) (bool, error) {
	out, _, err := program.Eval(vars)
	if err != nil {
		return false, fmt.Errorf("%w: %w: %w", domain.ErrEventRejected, ErrCELEvaluation, err)
	}
	keep, ok := out.Value().(bool)
	if !ok {
		return false, fmt.Errorf(
			"%w: %w: expression returned %T instead of bool",
			domain.ErrEventRejected,
			ErrCELEvaluation,
			out.Value(),
		)
	}
	return keep, nil
}

type activation struct {
	emptyResource *resourcepb.Resource
	emptyScope    *commonpb.InstrumentationScope

	resource  *resourcepb.Resource
	scope     *commonpb.InstrumentationScope
	record    *logspb.LogRecord
	metric    *metricspb.Metric
	dataPoint proto.Message
	span      *tracepb.Span
}

func (a *activation) set(resource *resourcepb.Resource, scope *commonpb.InstrumentationScope) {
	a.resource, a.scope = resource, scope
	if a.resource == nil {
		a.resource = a.emptyResource
	}
	if a.scope == nil {
		a.scope = a.emptyScope
	}
}

func (a *activation) ResolveName(name string) (any, bool) {
	switch name {
	case "resource":
		return a.resource, true
	case "scope":
		return a.scope, true
	case "record":
		return a.record, a.record != nil
	case "metric":
		return a.metric, a.metric != nil
	case "datapoint":
		return a.dataPoint, a.dataPoint != nil
	case "span":
		return a.span, a.span != nil
	default:
		return nil, false
	}
}

func (a *activation) Parent() interpreter.Activation {
	return nil
}
