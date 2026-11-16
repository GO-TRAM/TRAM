// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

// Package cloudevent receives CloudEvents carrying OTLP data through a message
// broker chosen by configuration.
package cloudevent

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/cloudevents/sdk-go/v2/binding"
	"github.com/go-playground/validator/v10"

	"codeberg.org/eljoth/tram/internal/adapters/cebroker"
	"codeberg.org/eljoth/tram/internal/adapters/source/base"
	"codeberg.org/eljoth/tram/internal/core/domain"
)

const (
	Type = "cloudevent"

	redeliveryDelay = 5 * time.Second

	// The protocol bindings hand over buffered messages one at a time while
	// they stop consuming. Once none arrived for this long, all were taken.
	drainIdle = 200 * time.Millisecond
)

type CloudEvent struct {
	*base.Source[Spec]

	receiver cebroker.Receiver
	logger   *slog.Logger
	wg       sync.WaitGroup

	stop   context.Context
	cancel context.CancelFunc

	mu      sync.Mutex
	running bool
	stopped bool
}

func New(source domain.SourceSpec, validator *validator.Validate, logger *slog.Logger) (*CloudEvent, error) {
	b, err := base.New[Spec](source.Name, source.Spec, validator)
	if err != nil {
		return nil, err
	}

	var receiver cebroker.Receiver
	switch b.Spec().Broker {
	case cebroker.BrokerNATS:
		receiver, err = cebroker.NewNATSReceiver(b.Spec().NATS, workersOf(b.Spec()), "source "+source.Name,
			logger.With(slog.String("source", source.Name)))
	default:
		err = fmt.Errorf("unsupported broker %q", b.Spec().Broker)
	}
	if err != nil {
		return nil, err
	}
	return newWithReceiver(b, receiver, logger), nil
}

func newWithReceiver(b *base.Source[Spec], receiver cebroker.Receiver, logger *slog.Logger) *CloudEvent {
	stop, cancel := context.WithCancel(context.Background())
	return &CloudEvent{
		Source:   b,
		receiver: receiver,
		logger:   logger,
		stop:     stop,
		cancel:   cancel,
	}
}

func workersOf(spec *Spec) int {
	return int(max(spec.Workers, 1))
}

// Run consumes until Stop. Deliveries use ctx, not the stop context, so events
// being handled when the source stops are still acknowledged.
func (c *CloudEvent) Run(ctx context.Context) error {
	c.mu.Lock()
	if c.stopped {
		c.mu.Unlock()
		return nil
	}
	c.running = true
	c.mu.Unlock()
	// The connection is closed once the workers are done, so messages still
	// being handled can be acknowledged.
	defer c.receiver.Close()

	consuming, stopConsuming := context.WithCancel(c.stop)
	defer stopConsuming()
	opened := make(chan error, 1)
	go func() {
		err := c.receiver.Open(consuming)
		// Open returns early if it could not start consuming, e.g. without
		// the stream; receiving would wait forever then.
		stopConsuming()
		opened <- err
	}()

	c.receive(ctx, consuming, workersOf(c.Spec()))
	stopConsuming()
	c.drain(ctx)
	c.wg.Wait()

	if err := <-opened; err != nil && c.stop.Err() == nil {
		return fmt.Errorf("failed to consume: %w", err)
	}
	return nil
}

// receive takes a message only once a worker is free, so the binding buffers
// at most as many as there are workers.
func (c *CloudEvent) receive(ctx, consuming context.Context, workers int) {
	slots := make(chan struct{}, workers)
	for {
		// With a slot free, select would pick at random between it and a
		// stop that already happened.
		if consuming.Err() != nil {
			return
		}
		select {
		case slots <- struct{}{}:
		case <-consuming.Done():
			return
		}
		m, err := c.receiver.Receive(consuming)
		if err != nil {
			<-slots
			return
		}
		c.wg.Go(func() {
			defer func() { <-slots }()
			c.handle(ctx, m)
		})
	}
}

// drain hands the messages the binding still buffers back to the broker:
// the binding blocks until each one is received, which would keep it from
// ever finishing to stop.
func (c *CloudEvent) drain(ctx context.Context) {
	for {
		idle, cancel := context.WithTimeout(context.Background(), drainIdle)
		m, err := c.receiver.Receive(idle)
		cancel()
		if err != nil {
			return
		}
		if err = c.receiver.Retry(m, 0); err != nil {
			c.logger.ErrorContext(ctx, "Could not return message while stopping", slog.Any("error", err))
		}
	}
}

func (c *CloudEvent) handle(ctx context.Context, m binding.Message) {
	event, err := c.eventOf(ctx, m)
	if err != nil {
		c.logger.WarnContext(ctx, "Rejected message that is not a valid OTLP CloudEvent", slog.Any("error", err))
		c.reject(ctx, m)
		return
	}

	err = c.NotifySubscribers(ctx, event)
	if errors.Is(err, domain.ErrEventRejected) {
		c.logger.WarnContext(ctx, "Rejected message", slog.Any("error", err))
		c.reject(ctx, m)
		return
	}
	if err != nil {
		c.logger.WarnContext(ctx, "Could not deliver message, requesting redelivery", slog.Any("error", err))
		if err = c.receiver.Retry(m, redeliveryDelay); err != nil {
			c.logger.ErrorContext(ctx, "Could not request redelivery of message", slog.Any("error", err))
		}
		return
	}

	if err = c.receiver.Ack(m); err != nil {
		c.logger.ErrorContext(ctx, "Could not acknowledge message", slog.Any("error", err))
	}
}

func (c *CloudEvent) eventOf(ctx context.Context, m binding.Message) (*domain.Event, error) {
	e, err := binding.ToEvent(ctx, m)
	if err != nil {
		return nil, err
	}
	return cebroker.EventOf(e)
}

func (c *CloudEvent) reject(ctx context.Context, m binding.Message) {
	if err := c.receiver.Reject(m); err != nil {
		c.logger.ErrorContext(ctx, "Could not reject message", slog.Any("error", err))
	}
}

func (c *CloudEvent) Stop() error {
	c.mu.Lock()
	c.stopped = true
	running := c.running
	c.mu.Unlock()

	c.cancel()
	if !running {
		return c.receiver.Close()
	}
	return nil
}
