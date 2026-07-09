package celfilter

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
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

const Type = "cel_filter"

type CELFilter struct {
	*base.Stage[Spec]
	celEnv     *cel.Env
	celProgram cel.Program
}

func New(stage domain.StageSpec, validator *validator.Validate) (*CELFilter, error) {
	base, err := base.New[Spec](stage.Name, stage.From, stage.Spec, validator)
	if err != nil {
		return nil, err
	}

	celEnv, err := cel.NewEnv(
		cel.TypeDescs(
			commonpb.File_opentelemetry_proto_common_v1_common_proto,
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

	ast, issues := celEnv.Compile(base.Spec().Expression)
	if issues != nil && issues.Err() != nil {
		return nil, fmt.Errorf("failed to compile CEL expression: %w", issues.Err())
	}

	celProgram, err := celEnv.Program(ast)
	if err != nil {
		return nil, fmt.Errorf("failed to build CEL program: %w", err)
	}

	return &CELFilter{
		Stage:      base,
		celEnv:     celEnv,
		celProgram: celProgram,
	}, nil
}

func (n *CELFilter) Notify(ctx context.Context, event *domain.Event) error {
	result, _, err := n.celProgram.Eval(map[string]any{
		"event": event,
	})

	if err != nil {
		return fmt.Errorf("CEL evaluation error: %w", err)
	}

	isMatch := result.Value().(bool)

	if !isMatch {
		switch n.Stage.Spec().OnMismatch {
		case "drop":
			return nil
		case "reject":
			fallthrough
		default:
			return fmt.Errorf("event did not match the CEL filter criteria")
		}
	}

	return n.NotifySubscribers(ctx, event)
}
