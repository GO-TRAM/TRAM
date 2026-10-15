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
	"sync"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"codeberg.org/eljoth/tram/internal/adapters/source/base"
	"codeberg.org/eljoth/tram/internal/core/domain"
)

const (
	Type = "nats"

	SignalHeader = "Tram-Signal"

	redeliveryDelay = 5 * time.Second

	// NATS' default for a consumer's max ack pending.
	defaultMaxAckPending = 1000
	// A worker's message being handled and one buffered for it.
	unackedPerWorker = 2
)

const (
	minRetryDelay = 100 * time.Millisecond
	maxRetryDelay = 5 * time.Second
	retryGrowth   = 2
)

type NATS struct {
	*base.Source[Spec]

	logger *slog.Logger
	nc     *nats.Conn
	js     jetstream.JetStream
	wg     sync.WaitGroup

	stop   context.Context
	cancel context.CancelFunc

	mu      sync.Mutex
	running bool
	stopped bool
}

func New(source domain.SourceSpec, validator *validator.Validate, logger *slog.Logger) (*NATS, error) {
	base, err := base.New[Spec](source.Name, source.Spec, validator)
	if err != nil {
		return nil, err
	}

	nc, err := nats.Connect(base.Spec().URL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("failed to initialize JetStream: %w", err)
	}

	stop, cancel := context.WithCancel(context.Background())
	return &NATS{
		Source: base,
		logger: logger,
		nc:     nc,
		js:     js,
		stop:   stop,
		cancel: cancel,
	}, nil
}

func (n *NATS) Run(ctx context.Context) error {
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return nil
	}
	n.running = true
	n.mu.Unlock()
	// The connection is closed once the workers are done, so messages still
	// being processed can be acknowledged.
	defer n.nc.Close()

	stream, err := n.js.Stream(ctx, n.Spec().Stream)
	if err != nil {
		return fmt.Errorf("failed to get stream %s: %w", n.Spec().Stream, err)
	}

	workers := uint(1)
	if n.Spec().Workers > 0 {
		workers = n.Spec().Workers
	}

	cons, err := stream.CreateOrUpdateConsumer(ctx, consumerConfig(n.Spec(), int(workers)))
	if err != nil {
		return fmt.Errorf("failed to create consumer: %w", err)
	}

	n.consume(ctx, cons, int(workers))
	return nil
}

// A source has up to twice its workers in messages unacknowledged: those being
// handled and as many buffered. NATS stops delivering at the consumer's max ack
// pending, 1,000 by default, so with more than 500 workers some of them would
// stay idle. It is only raised, since messages waiting for redelivery also
// count towards it.
func consumerConfig(spec *Spec, workers int) jetstream.ConsumerConfig {
	return jetstream.ConsumerConfig{
		Durable:       spec.Durable,
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		FilterSubject: spec.Subject,
		MaxAckPending: max(unackedPerWorker*workers, defaultMaxAckPending),
	}
}

// Messages are pulled continuously instead of one fetch after another, which
// keeps NATS from handling a new pull request for every few messages. Deliveries
// use ctx, not the stop context, so messages being handled when the source
// stops are still acked.
func (n *NATS) consume(ctx context.Context, cons jetstream.Consumer, workers int) {
	defer n.wg.Wait()

	slots := make(chan struct{}, workers)
	var delay time.Duration
	for {
		received, err := n.receive(ctx, cons, slots, workers)
		if n.stop.Err() != nil {
			return
		}
		if received {
			delay = 0
		}

		delay = min(max(retryGrowth*delay, minRetryDelay), maxRetryDelay)
		n.logger.ErrorContext(ctx, "Could not receive messages, retrying",
			slog.String("source", n.Name()), slog.Duration("delay", delay), slog.Any("error", err))

		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-n.stop.Done():
			timer.Stop()
		}
	}
}

// The client buffers at most as many messages as there are workers, so a
// buffered message waits for at most one round of handling while its ack wait
// runs. A message is only taken from the buffer once a worker is free. Returns
// whether any message was received before the iterator ended.
func (n *NATS) receive(
	ctx context.Context,
	cons jetstream.Consumer,
	slots chan struct{},
	workers int,
) (bool, error) {
	it, err := cons.Messages(jetstream.PullMaxMessages(workers))
	if err != nil {
		return false, err
	}
	// Messages still buffered are not acked and are redelivered after their
	// ack wait.
	defer it.Stop()

	received := false
	for n.acquire(slots) {
		msg, nextErr := it.Next(jetstream.NextContext(n.stop))
		if nextErr != nil {
			<-slots
			if n.stop.Err() != nil {
				return received, nil
			}
			// The iterator pulls again by itself after missed heartbeats, e.g.
			// while NATS is unreachable. Every other error ends it.
			if errors.Is(nextErr, jetstream.ErrNoHeartbeat) {
				n.logger.WarnContext(ctx, "Missed heartbeats from NATS",
					slog.String("source", n.Name()), slog.Any("error", nextErr))
				continue
			}
			return received, nextErr
		}
		received = true
		n.wg.Go(func() {
			defer func() { <-slots }()
			n.handle(ctx, msg)
		})
	}
	return received, nil
}

func (n *NATS) acquire(slots chan struct{}) bool {
	// With a slot free, select would pick at random between it and a stop
	// that already happened, and receive again.
	if n.stop.Err() != nil {
		return false
	}
	select {
	case slots <- struct{}{}:
		return true
	case <-n.stop.Done():
		return false
	}
}

func (n *NATS) handle(ctx context.Context, msg jetstream.Msg) {
	signal := domain.Signal(msg.Headers().Get(SignalHeader))
	event, err := domain.UnmarshalOTLPData(signal, msg.Data())
	if err != nil {
		n.logger.WarnContext(ctx, "Terminated message that is not valid OTLP data", slog.Any("error", err))
		n.term(ctx, msg)
		return
	}

	err = n.NotifySubscribers(ctx, event)
	if errors.Is(err, domain.ErrEventRejected) {
		n.logger.WarnContext(ctx, "Terminated rejected message", slog.Any("error", err))
		n.term(ctx, msg)
		return
	}
	if err != nil {
		n.logger.WarnContext(ctx, "Could not deliver message, requesting redelivery", slog.Any("error", err))
		if err = msg.NakWithDelay(redeliveryDelay); err != nil {
			n.logger.ErrorContext(ctx, "Could not request redelivery of message", slog.Any("error", err))
		}
		return
	}

	if err = msg.Ack(); err != nil {
		n.logger.ErrorContext(ctx, "Could not acknowledge message", slog.Any("error", err))
	}
}

func (n *NATS) term(ctx context.Context, msg jetstream.Msg) {
	if err := msg.Term(); err != nil {
		n.logger.ErrorContext(ctx, "Could not terminate message", slog.Any("error", err))
	}
}

func (n *NATS) Stop() error {
	n.mu.Lock()
	n.stopped = true
	running := n.running
	n.mu.Unlock()

	n.cancel()
	if !running {
		n.nc.Close()
	}
	return nil
}
