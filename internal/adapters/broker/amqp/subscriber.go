// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package amqp

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/Azure/go-amqp"

	"github.com/go-tram/tram/internal/adapters/broker"
)

const (
	// A worker's message being handled and one buffered for it.
	unsettledPerWorker = 2

	// How long Next waits before attaching again after receiving failed.
	minRetryDelay = 100 * time.Millisecond
	maxRetryDelay = 5 * time.Second
	retryGrowth   = 2

	// How long the broker may take to confirm a disposition.
	settleTimeout = 10 * time.Second
)

// subscriber reads one address. Next is not safe for concurrent use; the
// source calls it from one goroutine.
type subscriber struct {
	spec     *SubscriberSpec
	capacity int
	logger   *slog.Logger
	session  *session

	receiver   *amqp.Receiver
	generation int
	delay      time.Duration
}

func newSubscriber(spec *SubscriberSpec, cfg broker.SubscriberConfig, logger *slog.Logger) *subscriber {
	return &subscriber{
		spec:     spec,
		capacity: cfg.Capacity,
		logger:   logger,
		session:  newSession(spec.ConnectionSpec, cfg.Name, logger),
	}
}

// Open attaches the receiver link, which fails if the broker has no such
// address.
func (s *subscriber) Open(ctx context.Context) error {
	return s.attach(ctx)
}

// attach opens a receiver with credit for the messages being handled and as
// many buffered; go-amqp grants new credit as messages are settled.
func (s *subscriber) attach(ctx context.Context) error {
	sess, generation, err := s.session.get(ctx)
	if err != nil {
		return err
	}
	attachCtx, cancel := context.WithTimeout(ctx, connectTimeout)
	defer cancel()
	receiver, err := sess.NewReceiver(attachCtx, s.spec.Address, &amqp.ReceiverOptions{
		Credit: int32(unsettledPerWorker * s.capacity), //nolint:gosec // Workers are at most 10,000.
	})
	if err != nil {
		s.session.fail(generation, err)
		return fmt.Errorf("failed to attach to address %s: %w", s.spec.Address, err)
	}
	s.receiver, s.generation = receiver, generation
	return nil
}

func (s *subscriber) Next(ctx context.Context) (broker.Delivery, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		attached, err := s.ensureAttached(ctx)
		if err != nil {
			return nil, err
		}
		if !attached {
			continue
		}
		msg, err := s.receiver.Receive(ctx, nil)
		if err == nil {
			s.delay = 0
			return &delivery{receiver: s.receiver, msg: msg, address: s.spec.Address}, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		// Messages received on the lost link are not settled; the broker
		// delivers them again.
		s.session.fail(s.generation, err)
		s.receiver = nil
		if err = s.wait(ctx, err); err != nil {
			return nil, err
		}
	}
}

// ensureAttached attaches a receiver if none is attached. It reports false
// after a failed attach was waited out, so the caller tries again; the error
// is set only when ctx ends while waiting.
func (s *subscriber) ensureAttached(ctx context.Context) (bool, error) {
	if s.receiver != nil {
		return true, nil
	}
	if err := s.attach(ctx); err != nil {
		return false, s.wait(ctx, err)
	}
	return true, nil
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

// Close detaches and closes the connection. Messages not settled yet are
// delivered again by the broker.
func (s *subscriber) Close() error {
	return s.session.close()
}

type delivery struct {
	receiver *amqp.Receiver
	msg      *amqp.Message
	address  string
}

func (d *delivery) Body() []byte        { return d.msg.GetData() }
func (d *delivery) Destination() string { return d.address }

func (d *delivery) Ack() error {
	ctx, cancel := context.WithTimeout(context.Background(), settleTimeout)
	defer cancel()
	return d.receiver.AcceptMessage(ctx, d.msg)
}

// Reject settles the message as rejected; what happens to it then is the
// broker's policy, usually a dead-letter address.
func (d *delivery) Reject() error {
	ctx, cancel := context.WithTimeout(context.Background(), settleTimeout)
	defer cancel()
	return d.receiver.RejectMessage(ctx, d.msg, nil)
}

// Retry holds the message for delay and then returns it as a failed delivery:
// AMQP has no delayed redelivery, and a failed delivery counts towards the
// broker's limit of attempts before it dead-letters a message. The held
// message keeps its credit until then.
func (d *delivery) Retry(delay time.Duration) error {
	time.AfterFunc(delay, func() {
		ctx, cancel := context.WithTimeout(context.Background(), settleTimeout)
		defer cancel()
		_ = d.receiver.ModifyMessage(ctx, d.msg, &amqp.ModifyMessageOptions{DeliveryFailed: true})
	})
	return nil
}
