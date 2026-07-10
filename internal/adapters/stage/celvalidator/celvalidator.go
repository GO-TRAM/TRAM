package celvalidator

import (
	"context"
	"fmt"
	"reflect"

	"codeberg.org/eljoth/tram/internal/adapters/stage/base"
	"codeberg.org/eljoth/tram/internal/core/domain"
	"github.com/go-playground/validator/v10"
	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/ext"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

const Type = "cel_validator"

type CELFilter struct {
	*base.Stage[Spec]
	metrics cel.Program
	logs    cel.Program
	traces  cel.Program
}

func New(stage domain.StageSpec, validator *validator.Validate) (*CELFilter, error) {
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
			tracepb.File_opentelemetry_proto_trace_v1_trace_proto,
		),

		ext.NativeTypes(reflect.TypeOf(&domain.Event{})),

		cel.Variable("event", cel.ObjectType("domain.Event")),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create CEL environment: %w", err)
	}

	logsEnv, err := baseEnv.Extend(cel.Variable("logs", cel.ObjectType("opentelemetry.proto.logs.v1.LogsData")))
	if err != nil {
		return nil, err
	}

	logs, err := initCEL(logsEnv, base.Spec().LogsFilter)
	if err != nil {
		return nil, fmt.Errorf("failed to create CEL environment: %w", err)
	}

	metricsEnv, err := baseEnv.Extend(cel.Variable("metrics", cel.ObjectType("opentelemetry.proto.metrics.v1.MetricsData")))
	if err != nil {
		return nil, err
	}
	metrics, err := initCEL(metricsEnv, base.Spec().MetricsFilter)
	if err != nil {
		return nil, fmt.Errorf("failed to create CEL environment: %w", err)
	}

	tracesEnv, err := baseEnv.Extend(cel.Variable("traces", cel.ObjectType("opentelemetry.proto.trace.v1.TracesData")))
	if err != nil {
		return nil, err
	}
	traces, err := initCEL(tracesEnv, base.Spec().TracesFilter)
	if err != nil {
		return nil, fmt.Errorf("failed to create CEL environment: %w", err)
	}

	return &CELFilter{
		Stage:   base,
		logs:    logs,
		metrics: metrics,
		traces:  traces,
	}, nil
}

func initCEL(celEnv *cel.Env, expression string) (cel.Program, error) {
	if expression == "" {
		expression = "true"
	}
	ast, issues := celEnv.Compile(expression)
	if issues != nil && issues.Err() != nil {
		return nil, fmt.Errorf("failed to compile CEL expression: %w", issues.Err())
	}

	celProgram, err := celEnv.Program(ast)
	if err != nil {
		return nil, fmt.Errorf("failed to build CEL program: %w", err)
	}
	return celProgram, nil
}

func (n *CELFilter) filter(_ context.Context, varName string, data any, filter cel.Program, event *domain.Event, dropper func(*domain.Event)) error {
	result, _, err := filter.Eval(map[string]any{
		varName: data,
	})

	if err != nil {
		return fmt.Errorf("CEL evaluation error: %w", err)
	}

	isMatch := result.Value().(bool)

	if !isMatch {
		switch n.Stage.Spec().OnMismatch {
		case "drop":
			dropper(event)
			return nil
		case "reject":
			fallthrough
		default:
			return fmt.Errorf("event did not match the CEL filter criteria")
		}
	}
	return nil
}

func (n *CELFilter) Notify(ctx context.Context, event *domain.Event) error {
	if event.Logs != nil {
		if err := n.filter(ctx, "logs", event.Logs, n.logs, event, func(e *domain.Event) { e.Logs = nil }); err != nil {
			return err
		}
	}

	if event.Metrics != nil {
		if err := n.filter(ctx, "metrics", event.Metrics, n.metrics, event, func(e *domain.Event) { e.Metrics = nil }); err != nil {
			return err
		}
	}

	if event.Traces != nil {
		if err := n.filter(ctx, "traces", event.Traces, n.traces, event, func(e *domain.Event) { e.Traces = nil }); err != nil {
			return err
		}
	}

	if event.Logs == nil && event.Metrics == nil && event.Traces == nil {
		return nil
	}

	return n.NotifySubscribers(ctx, event)
}
