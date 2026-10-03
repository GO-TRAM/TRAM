package celvalidator

import (
	"context"
	"errors"
	"fmt"

	"github.com/go-playground/validator/v10"
	"github.com/google/cel-go/cel"

	"codeberg.org/eljoth/tram/internal/adapters/stage/base"
	"codeberg.org/eljoth/tram/internal/core/domain"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracespb "go.opentelemetry.io/proto/otlp/trace/v1"
)

const Type = "cel_validator"

type CELValidator struct {
	*base.Stage[Spec]

	metrics cel.Program
	logs    cel.Program
	traces  cel.Program
}

func New(stage domain.StageSpec, validator *validator.Validate) (*CELValidator, error) {
	base, err := base.New[Spec](stage.Name, stage.From, stage.Spec, validator)
	if err != nil {
		return nil, err
	}

	baseEnv, err := cel.NewEnv(
		cel.TypeDescs(
			commonpb.File_opentelemetry_proto_common_v1_common_proto,
			resourcepb.File_opentelemetry_proto_resource_v1_resource_proto,
			logspb.File_opentelemetry_proto_logs_v1_logs_proto,
			metricspb.File_opentelemetry_proto_metrics_v1_metrics_proto,
			tracespb.File_opentelemetry_proto_trace_v1_trace_proto,
		),
	)
	if err != nil {
		return nil, errors.Join(ErrCELEnvironmenInitialization, err)
	}

	logs, err := initCEL(
		baseEnv,
		base.Spec().Logs,
		cel.Variable("logs", cel.ObjectType("opentelemetry.proto.logs.v1.LogsData")),
	)
	if err != nil {
		return nil, errors.Join(ErrCELInvalidLogsExpression, err)
	}

	metrics, err := initCEL(
		baseEnv,
		base.Spec().Metrics,
		cel.Variable("metrics", cel.ObjectType("opentelemetry.proto.metrics.v1.MetricsData")),
	)
	if err != nil {
		return nil, errors.Join(ErrCELInvalidMetricsExpression, err)
	}

	traces, err := initCEL(
		baseEnv,
		base.Spec().Traces,
		cel.Variable("traces", cel.ObjectType("opentelemetry.proto.trace.v1.TracesData")),
	)
	if err != nil {
		return nil, errors.Join(ErrCELInvalidTracesExpression, err)
	}

	return &CELValidator{
		Stage:   base,
		logs:    logs,
		metrics: metrics,
		traces:  traces,
	}, nil
}

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

func (n *CELValidator) validate(
	_ context.Context,
	varName string,
	data any,
	filter cel.Program,
	event *domain.Event,
	dropper func(*domain.Event),
) error {
	result, _, err := filter.Eval(map[string]any{
		varName: data,
	})

	// Evaluating the same event again fails the same way, so errors reject
	// the event instead of asking for a retry.
	if err != nil {
		return fmt.Errorf("%w: %w: %w", domain.ErrEventRejected, ErrCELEvaluation, err)
	}

	isMatch, ok := result.Value().(bool)
	if !ok {
		return fmt.Errorf("%w: %w: expression returned %T instead of bool",
			domain.ErrEventRejected, ErrCELEvaluation, result.Value())
	}

	if !isMatch {
		switch n.Stage.Spec().OnMismatch {
		case OnMismatchDrop:
			dropper(event)
			return nil
		case OnMismatchReject:
			fallthrough
		default:
			return fmt.Errorf("%w: event did not match the CEL filter criteria", domain.ErrEventRejected)
		}
	}
	return nil
}

func (n *CELValidator) Notify(ctx context.Context, event *domain.Event) error {
	if n == nil {
		return ErrCELEnvironmenInitialization
	}

	// The incoming event is shared with sibling branches, so drops are applied
	// to a shallow copy.
	copied := *event
	event = &copied

	if event.Logs != nil {
		if err := n.validate(
			ctx,
			"logs",
			event.Logs,
			n.logs,
			event,
			func(e *domain.Event) { e.Logs = nil },
		); err != nil {
			return errors.Join(ErrCELRejectingLogs, err)
		}
	}

	if event.Metrics != nil {
		if err := n.validate(
			ctx,
			"metrics",
			event.Metrics,
			n.metrics,
			event,
			func(e *domain.Event) { e.Metrics = nil },
		); err != nil {
			return errors.Join(ErrCELRejectingMetrics, err)
		}
	}

	if event.Traces != nil {
		if err := n.validate(
			ctx,
			"traces",
			event.Traces,
			n.traces,
			event,
			func(e *domain.Event) { e.Traces = nil },
		); err != nil {
			return errors.Join(ErrCELRejectingTraces, err)
		}
	}

	if event.Logs == nil && event.Metrics == nil && event.Traces == nil {
		return nil
	}

	return n.NotifySubscribers(ctx, event)
}
