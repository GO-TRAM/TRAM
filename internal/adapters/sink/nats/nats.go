// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

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

	SignalHeader = "Tram-Signal"

	logsVariable    = "logs"
	metricsVariable = "metrics"
	tracesVariable  = "traces"

	resourceVariable = "resource"
	scopeVariable    = "scope"
	recordVariable   = "record"

	metricVariable    = "metric"
	dataPointVariable = "datapoint"
	spanVariable      = "span"

	getAttrArguments = 2

	// Without it, a publish whose acknowledgement is lost, e.g. while NATS
	// restarts, would wait forever when the source's context has no deadline.
	publishAckTimeout = 10 * time.Second

	// Bounds the messages a sink has in flight. NATS stores the messages of a
	// stream one after another and buffers what it has not stored yet, so
	// without a bound a fast sink can make it run out of memory. A sink at the
	// bound waits instead, which slows its source down.
	maxPendingPublishes = 256
)

type NATS struct {
	*base.Sink[Spec]
	subjects

	logger    *slog.Logger
	natsConn  *nats.Conn
	jetstream jetstream.JetStream
	stream    jetstream.Stream
}

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

	compiled, err := compileSubjects(base.Spec().Subject)
	if err != nil {
		return nil, err
	}

	nc, err := nats.Connect(base.Spec().URL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}

	js, err := jetstream.New(nc,
		jetstream.WithPublishAsyncTimeout(publishAckTimeout),
		jetstream.WithPublishAsyncMaxPending(maxPendingPublishes))
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("failed to initialize JetStream: %w", err)
	}

	stream, err := setUpStream(context.Background(), js, base.Spec(), logger)
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
		cel.Variable(resourceVariable, cel.ObjectType("opentelemetry.proto.resource.v1.Resource")),
		cel.Variable(scopeVariable, cel.ObjectType("opentelemetry.proto.common.v1.InstrumentationScope")),
		cel.Variable(recordVariable, cel.ObjectType("opentelemetry.proto.logs.v1.LogRecord")),
	)
	if err != nil {
		return subjects{}, errors.Join(ErrCELInvalidLogsExpression, err)
	}

	compiled.metrics, err = initCEL(
		baseEnv,
		subject.Metrics,
		cel.Variable(metricsVariable, cel.ObjectType("opentelemetry.proto.metrics.v1.MetricsData")),
		cel.Variable(resourceVariable, cel.ObjectType("opentelemetry.proto.resource.v1.Resource")),
		cel.Variable(scopeVariable, cel.ObjectType("opentelemetry.proto.common.v1.InstrumentationScope")),
		cel.Variable(metricVariable, cel.ObjectType("opentelemetry.proto.metrics.v1.Metric")),
		// Its type depends on the metric's type.
		cel.Variable(dataPointVariable, cel.DynType),
	)
	if err != nil {
		return subjects{}, errors.Join(ErrCELInvalidMetricsExpression, err)
	}

	compiled.traces, err = initCEL(
		baseEnv,
		subject.Traces,
		cel.Variable(tracesVariable, cel.ObjectType("opentelemetry.proto.trace.v1.TracesData")),
		cel.Variable(resourceVariable, cel.ObjectType("opentelemetry.proto.resource.v1.Resource")),
		cel.Variable(scopeVariable, cel.ObjectType("opentelemetry.proto.common.v1.InstrumentationScope")),
		cel.Variable(spanVariable, cel.ObjectType("opentelemetry.proto.trace.v1.Span")),
	)
	if err != nil {
		return subjects{}, errors.Join(ErrCELInvalidTracesExpression, err)
	}
	return compiled, nil
}

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
		// Resolving the subjects of the same event fails the same way again,
		// so the event is rejected instead of retried. It must not count as
		// delivered either, which would lose it silently.
		n.logger.WarnContext(ctx, "Could not resolve subject, rejecting event",
			slog.String("sink", n.Name()), slog.Any("error", err))
		return fmt.Errorf("%w: could not resolve subject: %w", domain.ErrEventRejected, err)
	}
	if err = n.publish(ctx, messages); err != nil {
		return fmt.Errorf("failed to publish event: %w", err)
	}
	return nil
}

// An event resolves to one message per subject, which with many subjects is
// one per record. Publishing them one after another would wait for every
// acknowledgement in turn, so all are sent first and acknowledged together.
// Each message is sent as soon as it is encoded: NATS stores the first ones
// while the rest are still being encoded, and only the async publish window of
// encoded messages is held at once instead of the whole event a second time.
func (n *NATS) publish(ctx context.Context, messages []message) error {
	acks := make([]jetstream.PubAckFuture, 0, len(messages))
	var err error
	for _, m := range messages {
		signal, data, marshalErr := m.event.MarshalOTLPData()
		if marshalErr != nil {
			err = fmt.Errorf("failed to marshal event: %w", marshalErr)
			break
		}
		ack, publishErr := n.jetstream.PublishMsgAsync(&nats.Msg{
			Subject: m.subject,
			Data:    data,
			Header:  nats.Header{SignalHeader: []string{string(signal)}},
		}, jetstream.WithStallWait(publishAckTimeout))
		if publishErr != nil {
			err = publishErr
			break
		}
		acks = append(acks, ack)
	}
	for _, ack := range acks {
		select {
		case <-ack.Ok():
		case ackErr := <-ack.Err():
			err = errors.Join(err, ackErr)
		case <-ctx.Done():
			return errors.Join(err, ctx.Err())
		}
	}
	return err
}

func (n *NATS) Stop() error {
	return n.natsConn.Drain()
}
