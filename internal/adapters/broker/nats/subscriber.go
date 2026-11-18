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

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"codeberg.org/eljoth/tram/internal/adapters/broker"
)

const (
	// NATS' default for a consumer's max ack pending.
	defaultMaxAckPending = 1000
	// A worker's message being handled and one buffered for it.
	unackedPerWorker = 2
)

// How long Next waits before pulling again after the iterator ended.
const (
	minRetryDelay = 100 * time.Millisecond
	maxRetryDelay = 5 * time.Second
	retryGrowth   = 2
)

var errNotOpen = errors.New("subscriber is not open")

// subscriber reads one consumer. Next is not safe for concurrent use; the
// source calls it from one goroutine and hands the deliveries to its workers.
type subscriber struct {
	spec     *SubscriberSpec
	capacity int
	logger   *slog.Logger
	conn     *nats.Conn
	js       jetstream.JetStream

	consumer jetstream.Consumer
	// it is the pull iterator, nil until the first Next and after it ended.
	it    jetstream.MessagesContext
	delay time.Duration
}

func newSubscriber(spec *SubscriberSpec, cfg broker.SubscriberConfig, logger *slog.Logger) (*subscriber, error) {
	conn, err := connect(spec.URL, cfg.Name, logger)
	if err != nil {
		return nil, err
	}
	js, err := jetstream.New(conn)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to initialize JetStream: %w", err)
	}
	return &subscriber{spec: spec, capacity: cfg.Capacity, logger: logger, conn: conn, js: js}, nil
}

// Open creates or updates the consumer on the stream that holds the subject.
func (s *subscriber) Open(ctx context.Context) error {
	name := s.spec.Stream
	if name == "" {
		var err error
		if name, err = s.js.StreamNameBySubject(ctx, s.spec.Subject); err != nil {
			return fmt.Errorf("failed to find the stream of subject %s: %w", s.spec.Subject, err)
		}
	}
	stream, err := s.js.Stream(ctx, name)
	if err != nil {
		return fmt.Errorf("failed to get stream %s: %w", name, err)
	}
	consumer, err := stream.CreateOrUpdateConsumer(ctx, consumerConfig(s.spec, s.capacity))
	if err != nil {
		return fmt.Errorf("failed to create consumer: %w", err)
	}
	s.consumer = consumer
	return nil
}

// A subscriber has up to twice its capacity in messages unacknowledged: those
// being handled and as many buffered. NATS stops delivering at the consumer's
// max ack pending, 1,000 by default, so with more than 500 workers some of
// them would stay idle. It is only raised, since messages waiting for
// redelivery also count towards it.
func consumerConfig(spec *SubscriberSpec, capacity int) jetstream.ConsumerConfig {
	return jetstream.ConsumerConfig{
		Durable:       spec.Durable,
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		FilterSubject: spec.Subject,
		MaxAckPending: max(unackedPerWorker*capacity, defaultMaxAckPending),
	}
}

// Next pulls continuously instead of one fetch after another, which keeps NATS
// from handling a new pull request for every few messages. The iterator
// buffers at most capacity messages, so a buffered message waits for at most
// one round of handling while its ack wait runs.
func (s *subscriber) Next(ctx context.Context) (broker.Delivery, error) {
	if s.consumer == nil {
		return nil, errNotOpen
	}
	for {
		it, err := s.iterator(ctx)
		if err != nil {
			return nil, err
		}
		msg, err := it.Next(jetstream.NextContext(ctx))
		if err == nil {
			s.delay = 0
			return delivery{msg}, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		// The iterator pulls again by itself after missed heartbeats, e.g.
		// while NATS is unreachable. Every other error ends it.
		if errors.Is(err, jetstream.ErrNoHeartbeat) {
			s.logger.WarnContext(ctx, "Missed heartbeats from NATS", slog.Any("error", err))
			continue
		}
		it.Stop()
		s.it = nil
		if err = s.wait(ctx, err); err != nil {
			return nil, err
		}
	}
}

// iterator returns the pull iterator, starting one if there is none. It
// returns an error only once ctx ends.
func (s *subscriber) iterator(ctx context.Context) (jetstream.MessagesContext, error) {
	for s.it == nil {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		it, err := s.consumer.Messages(jetstream.PullMaxMessages(s.capacity))
		if err == nil {
			s.it = it
			break
		}
		if err = s.wait(ctx, err); err != nil {
			return nil, err
		}
	}
	return s.it, nil
}

// wait logs why receiving failed and waits a growing delay before the next
// try, or until ctx ends.
func (s *subscriber) wait(ctx context.Context, cause error) error {
	s.delay = min(max(retryGrowth*s.delay, minRetryDelay), maxRetryDelay)
	s.logger.ErrorContext(ctx, "Could not receive messages, retrying",
		slog.Duration("delay", s.delay), slog.Any("error", cause))
	timer := time.NewTimer(s.delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close stops pulling and closes the connection. Messages still buffered are
// not acknowledged and are redelivered after their ack wait.
func (s *subscriber) Close() error {
	if s.it != nil {
		s.it.Stop()
		s.it = nil
	}
	s.conn.Close()
	return nil
}

type delivery struct{ msg jetstream.Msg }

func (d delivery) Body() []byte                    { return d.msg.Data() }
func (d delivery) Destination() string             { return d.msg.Subject() }
func (d delivery) Ack() error                      { return d.msg.Ack() }
func (d delivery) Retry(delay time.Duration) error { return d.msg.NakWithDelay(delay) }
func (d delivery) Reject() error                   { return d.msg.Term() }
