package nats

import (
	"context"
	"errors"
	"fmt"
	"log"
	"reflect"
	"sync"
	"time"

	"codeberg.org/eljoth/tram/internal/adapters/sink/base"
	"codeberg.org/eljoth/tram/internal/core/domain"
	"github.com/go-playground/validator/v10"
	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/ext"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracespb "go.opentelemetry.io/proto/otlp/trace/v1"
)

const (
	Type = "nats"
)

type NATS struct {
	*base.Sink[Spec]
	natsConn  *nats.Conn
	jetstream jetstream.JetStream
	wg        sync.WaitGroup
	stream    jetstream.Stream

	metrics cel.Program
	logs    cel.Program
	traces  cel.Program
}

func New(sink domain.SinkSpec, validator *validator.Validate) (*NATS, error) {
	base, err := base.New[Spec](sink.Name, sink.From, sink.Spec, validator)
	if err != nil {
		return nil, err
	}

	nc, err := nats.Connect(base.Spec().URL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize JetStream: %w", err)
	}

	jetstreamConfig := jetstream.StreamConfig{
		Name:     base.Spec().Stream,
		Subjects: base.Spec().Subject.Register,
	}

	if retentionStr := base.Spec().Retention; retentionStr != "" {
		retention, err := time.ParseDuration(base.Spec().Retention)
		if err != nil {
			return nil, err
		}
		jetstreamConfig.MaxAge = retention
	}

	if byteSize := base.Spec().Size; byteSize != 0 {
		jetstreamConfig.MaxBytes = byteSize
	}

	s, err := js.CreateOrUpdateStream(context.Background(), jetstreamConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create stream: %w", err)
	}

	getAttrFunc := cel.Function("get_attr",
		cel.MemberOverload("attributes_get_attr",
			[]*cel.Type{
				cel.ListType(cel.ObjectType("opentelemetry.proto.common.v1.KeyValue")),
				cel.StringType,
			},
			cel.StringType,
			cel.FunctionBinding(func(args ...ref.Val) ref.Val {
				if len(args) != 2 {
					return types.NewErr("get_attr expects exactly 2 arguments")
				}

				goList, err := args[0].ConvertToNative(reflect.TypeOf([]*commonpb.KeyValue{}))
				if err != nil {
					return types.NewErr("failed to convert attributes to Go slice: %v", err)
				}

				list, ok := goList.([]*commonpb.KeyValue)
				if !ok {
					return types.NewErr("attributes is not of type []*KeyValue")
				}

				searchKey, ok := args[1].Value().(string)
				if !ok {
					return types.NewErr("invalid key type")
				}

				for _, kv := range list {
					if kv.Key == searchKey {
						return types.String(kv.Value.GetStringValue())
					}
				}

				return types.String("")
			}),
		),
	)

	baseEnv, err := cel.NewEnv(
		cel.TypeDescs(
			commonpb.File_opentelemetry_proto_common_v1_common_proto,
			resourcepb.File_opentelemetry_proto_resource_v1_resource_proto,
			logspb.File_opentelemetry_proto_logs_v1_logs_proto,
			metricspb.File_opentelemetry_proto_metrics_v1_metrics_proto,
			tracespb.File_opentelemetry_proto_trace_v1_trace_proto,
		),
		ext.NativeTypes(reflect.TypeOf(&domain.Event{})),
		cel.Variable("event", cel.ObjectType("domain.Event")),
		getAttrFunc,
	)
	if err != nil {
		return nil, errors.Join(ErrCELEnvironmenInitialization, err)
	}

	logs, err := initCEL(baseEnv, base.Spec().Subject.Logs, cel.Variable("logs", cel.ObjectType("opentelemetry.proto.logs.v1.LogsData")))
	if err != nil {
		return nil, errors.Join(ErrCELInvalidLogsExpression, err)
	}

	metrics, err := initCEL(baseEnv, base.Spec().Subject.Metrics, cel.Variable("metrics", cel.ObjectType("opentelemetry.proto.metrics.v1.MetricsData")))
	if err != nil {
		return nil, errors.Join(ErrCELInvalidMetricsExpression, err)
	}

	traces, err := initCEL(baseEnv, base.Spec().Subject.Traces, cel.Variable("traces", cel.ObjectType("opentelemetry.proto.trace.v1.TracesData")))
	if err != nil {
		return nil, errors.Join(ErrCELInvalidTracesExpression, err)
	}

	return &NATS{
		Sink:      base,
		natsConn:  nc,
		jetstream: js,
		stream:    s,
		logs:      logs,
		metrics:   metrics,
		traces:    traces,
	}, nil
}

func (n *NATS) Notify(ctx context.Context, event *domain.Event) error {
	mappedEvents, err := n.groupEventsBySubjects(event)
	if err != nil {
		log.Printf("Failed to resolve subject template: %v. Dropping event.", err)
		return nil
	}

	for k, v := range mappedEvents {
		data, err := v.MarshalOTLPData()
		if err != nil {
			log.Println(err)
			return fmt.Errorf("failed to marshal event: %w", err)
		}
		_, err = n.jetstream.Publish(ctx, k, data)
		if err != nil {
			log.Println(err)
			return fmt.Errorf("failed to publish event: %w", err)
		}
	}

	return nil
}

func (n *NATS) Stop() error {
	return n.natsConn.Drain()
}
