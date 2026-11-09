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
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/google/cel-go/cel"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"codeberg.org/eljoth/tram/internal/adapters/natsconn"
	"codeberg.org/eljoth/tram/internal/adapters/natspartition"
	"codeberg.org/eljoth/tram/internal/adapters/sink/base"
	"codeberg.org/eljoth/tram/internal/adapters/sink/celentry"
	"codeberg.org/eljoth/tram/internal/core/domain"
)

const (
	Type = "nats"

	SignalHeader = "Tram-Signal"

	logsVariable    = "logs"
	metricsVariable = "metrics"
	tracesVariable  = "traces"

	partitionVariable = "partition"

	// Without it, a publish whose acknowledgement is lost, e.g. while NATS
	// restarts, would wait forever when the source's context has no deadline.
	publishAckTimeout = 10 * time.Second

	// Bounds the messages a sink has in flight. NATS stores the messages of a
	// stream one after another and buffers what it has not stored yet, so
	// without a bound a fast sink can make it run out of memory. A sink at the
	// bound waits instead, which slows its source down.
	defaultMaxPending = 256
)

type NATS struct {
	*base.Sink[Spec]
	subjects

	// Nil without partitioning.
	partitions *partitions

	logger    *slog.Logger
	natsConn  *nats.Conn
	jetstream jetstream.JetStream
	streams   []jetstream.Stream
}

type subjects struct {
	logs    cel.Program
	metrics cel.Program
	traces  cel.Program
}

type partitions struct {
	count int
	// keys returns the key an entry is partitioned by.
	keys subjects
	// registers holds the subjects of each partition's stream.
	registers [][]string
}

func New(sink domain.SinkSpec, validator *validator.Validate, logger *slog.Logger) (*NATS, error) {
	base, err := base.New[Spec](sink.Name, sink.From, sink.Spec, validator)
	if err != nil {
		return nil, err
	}

	compiled, parts, err := compile(base.Spec())
	if err != nil {
		return nil, err
	}

	nc, err := natsconn.Connect(base.Spec().URL, "sink "+sink.Name, logger.With(slog.String("sink", sink.Name)))
	if err != nil {
		return nil, err
	}

	js, err := jetstream.New(nc,
		jetstream.WithPublishAsyncTimeout(publishAckTimeout),
		jetstream.WithPublishAsyncMaxPending(maxPendingPublishes(base.Spec())))
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("failed to initialize JetStream: %w", err)
	}

	streams, err := setUpStreams(context.Background(), js, base.Spec(), logger)
	if err != nil {
		nc.Close()
		return nil, err
	}

	return &NATS{
		Sink:       base,
		subjects:   compiled,
		partitions: parts,
		logger:     logger,
		natsConn:   nc,
		jetstream:  js,
		streams:    streams,
	}, nil
}

// maxPendingPublishes returns how many messages the sink publishes before it
// waits for their acknowledgements.
func maxPendingPublishes(spec *Spec) int {
	if spec.MaxPending != 0 {
		return spec.MaxPending
	}
	return defaultMaxPending
}

func compile(spec *Spec) (subjects, *partitions, error) {
	if err := checkPartitioning(spec); err != nil {
		return subjects{}, nil, err
	}
	compiled, err := compileSubjects(spec.Subject, spec.Partition != nil)
	if err != nil || spec.Partition == nil {
		return compiled, nil, err
	}

	keys, err := compileKeys(*spec.Partition)
	if err != nil {
		return subjects{}, nil, err
	}
	parts := &partitions{count: spec.Partition.Count, keys: keys}
	for _, t := range targetsOf(spec) {
		parts.registers = append(parts.registers, t.subjects)
	}
	return compiled, parts, nil
}

// checkPartitioning checks that the placeholder is used exactly where
// partitioning needs it, and that every published signal has a key.
func checkPartitioning(spec *Spec) error {
	p := spec.Partition
	if p == nil {
		if natspartition.HasPlaceholder(spec.Stream) {
			return fmt.Errorf("%w: stream %s has %s, but partition is not set",
				ErrInvalidPartition, spec.Stream, natspartition.Placeholder)
		}
		for _, subject := range spec.Subject.Register {
			if natspartition.HasPlaceholder(subject) {
				return fmt.Errorf("%w: register subject %s has %s, but partition is not set",
					ErrInvalidPartition, subject, natspartition.Placeholder)
			}
		}
		return nil
	}

	if !natspartition.HasPlaceholder(spec.Stream) {
		return fmt.Errorf("%w: stream %s needs %s to name one stream per partition",
			ErrInvalidPartition, spec.Stream, natspartition.Placeholder)
	}
	for _, subject := range spec.Subject.Register {
		if !natspartition.HasPlaceholder(subject) {
			return fmt.Errorf("%w: register subject %s needs %s, since streams cannot share subjects",
				ErrInvalidPartition, subject, natspartition.Placeholder)
		}
	}
	for _, signal := range []struct{ name, subject, key string }{
		{logsVariable, spec.Subject.Logs, p.Logs},
		{metricsVariable, spec.Subject.Metrics, p.Metrics},
		{tracesVariable, spec.Subject.Traces, p.Traces},
	} {
		switch {
		case signal.subject != "" && signal.key == "":
			return fmt.Errorf("%w: %s are published, but partition.%s is not set",
				ErrInvalidPartition, signal.name, signal.name)
		case signal.subject == "" && signal.key != "":
			return fmt.Errorf("%w: partition.%s is set, but %s are not published",
				ErrInvalidPartition, signal.name, signal.name)
		}
	}
	return nil
}

func compileKeys(p PartitionSpec) (subjects, error) {
	keys, err := compileSubjects(SubjectSpec{Logs: p.Logs, Metrics: p.Metrics, Traces: p.Traces}, false)
	if err != nil {
		return subjects{}, fmt.Errorf("%w: key: %w", ErrInvalidPartition, err)
	}
	return keys, nil
}

// compileSubjects compiles an expression per signal, with the variables of its
// entries. With partitioned, the expressions can read the entry's partition.
func compileSubjects(subject SubjectSpec, partitioned bool) (subjects, error) {
	var extra []cel.EnvOption
	if partitioned {
		extra = append(extra, cel.Variable(partitionVariable, cel.IntType))
	}
	programs, err := celentry.Compile(
		celentry.Expressions{Logs: subject.Logs, Metrics: subject.Metrics, Traces: subject.Traces}, extra...)
	if err != nil {
		return subjects{}, err
	}
	return subjects{logs: programs.Logs, metrics: programs.Metrics, traces: programs.Traces}, nil
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
