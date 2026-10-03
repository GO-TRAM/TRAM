package nats

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"codeberg.org/eljoth/tram/internal/adapters/sink/base"
	"codeberg.org/eljoth/tram/internal/core/domain"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracespb "go.opentelemetry.io/proto/otlp/trace/v1"
)

const (
	Type = "nats"

	// SignalHeader carries the domain.Signal of the published payload. It must
	// match the header the nats source reads.
	SignalHeader = "Tram-Signal"

	// Names of the variables the subject expressions see.
	logsVariable    = "logs"
	metricsVariable = "metrics"
	tracesVariable  = "traces"

	// getAttrArguments are the receiver and the key of get_attr.
	getAttrArguments = 2
)

type NATS struct {
	*base.Sink[Spec]
	subjects

	logger    *slog.Logger
	natsConn  *nats.Conn
	jetstream jetstream.JetStream
	stream    jetstream.Stream
}

// subjects holds the compiled subject expressions per signal type.
type subjects struct {
	logs    cel.Program
	metrics cel.Program
	traces  cel.Program
}

func New(sink domain.SinkSpec, validator *validator.Validate, logger *slog.Logger) (*NATS, error) {
	base, err := base.New[Spec](sink.Name, sink.From, sink.Spec, validator)
	if err != nil {
		return nil, err
	}

	// Compiled before connecting, so invalid expressions fail without
	// touching NATS.
	compiled, err := compileSubjects(base.Spec().Subject)
	if err != nil {
		return nil, err
	}

	nc, err := nats.Connect(base.Spec().URL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}

	js, stream, err := setUpStream(nc, base.Spec())
	if err != nil {
		nc.Close()
		return nil, err
	}

	return &NATS{
		Sink:      base,
		subjects:  compiled,
		logger:    logger,
		natsConn:  nc,
		jetstream: js,
		stream:    stream,
	}, nil
}

// setUpStream creates the JetStream stream or updates it to the spec.
func setUpStream(nc *nats.Conn, spec *Spec) (jetstream.JetStream, jetstream.Stream, error) {
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to initialize JetStream: %w", err)
	}

	config := jetstream.StreamConfig{
		Name:     spec.Stream,
		Subjects: spec.Subject.Register,
	}
	if spec.Retention != "" {
		retention, parseErr := time.ParseDuration(spec.Retention)
		if parseErr != nil {
			return nil, nil, fmt.Errorf("invalid retention: %w", parseErr)
		}
		config.MaxAge = retention
	}
	if spec.Size != 0 {
		config.MaxBytes = spec.Size
	}

	stream, err := js.CreateOrUpdateStream(context.Background(), config)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create stream: %w", err)
	}
	return js, stream, nil
}

func compileSubjects(subject SubjectSpec) (subjects, error) {
	getAttrFunc := cel.Function("get_attr",
		cel.MemberOverload("attributes_get_attr",
			[]*cel.Type{
				cel.ListType(cel.ObjectType("opentelemetry.proto.common.v1.KeyValue")),
				cel.StringType,
			},
			cel.StringType,
			cel.FunctionBinding(getAttr),
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
		getAttrFunc,
	)
	if err != nil {
		return subjects{}, errors.Join(ErrCELEnvironmenInitialization, err)
	}

	var compiled subjects
	compiled.logs, err = initCEL(
		baseEnv,
		subject.Logs,
		cel.Variable(logsVariable, cel.ObjectType("opentelemetry.proto.logs.v1.LogsData")),
	)
	if err != nil {
		return subjects{}, errors.Join(ErrCELInvalidLogsExpression, err)
	}

	compiled.metrics, err = initCEL(
		baseEnv,
		subject.Metrics,
		cel.Variable(metricsVariable, cel.ObjectType("opentelemetry.proto.metrics.v1.MetricsData")),
	)
	if err != nil {
		return subjects{}, errors.Join(ErrCELInvalidMetricsExpression, err)
	}

	compiled.traces, err = initCEL(
		baseEnv,
		subject.Traces,
		cel.Variable(tracesVariable, cel.ObjectType("opentelemetry.proto.trace.v1.TracesData")),
	)
	if err != nil {
		return subjects{}, errors.Join(ErrCELInvalidTracesExpression, err)
	}
	return compiled, nil
}

// getAttr implements get_attr: the string value of the attribute with the
// given key, or "" if there is none.
func getAttr(args ...ref.Val) ref.Val {
	if len(args) != getAttrArguments {
		return types.NewErr("get_attr expects exactly 2 arguments")
	}

	goList, err := args[0].ConvertToNative(reflect.TypeFor[[]*commonpb.KeyValue]())
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
		if kv.GetKey() == searchKey {
			return types.String(kv.GetValue().GetStringValue())
		}
	}
	return types.String("")
}

func (n *NATS) Notify(ctx context.Context, event *domain.Event) error {
	messages, err := n.groupEventsBySubjects(event)
	if err != nil {
		n.logger.ErrorContext(ctx, "Could not resolve subject, dropping event",
			slog.String("sink", n.Name()), slog.Any("error", err))
		return nil
	}

	for _, m := range messages {
		signal, data, marshalErr := m.event.MarshalOTLPData()
		if marshalErr != nil {
			return fmt.Errorf("failed to marshal event: %w", marshalErr)
		}
		msg := &nats.Msg{
			Subject: m.subject,
			Data:    data,
			Header:  nats.Header{SignalHeader: []string{string(signal)}},
		}
		if _, err = n.jetstream.PublishMsg(ctx, msg); err != nil {
			return fmt.Errorf("failed to publish event: %w", err)
		}
	}

	return nil
}

func (n *NATS) Stop() error {
	return n.natsConn.Drain()
}
