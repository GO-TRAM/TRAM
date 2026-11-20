// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

// Package consume runs the subscriber of a source that reads from a broker
// or a buffer: it hands messages to a number of workers, passes them on as
// events and ends every delivery: acknowledged once delivered, rejected if
// the data is invalid or a node below rejects it for good, and retried after
// a delay otherwise.
package consume

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	contract "github.com/go-tram/tram/internal/adapters/broker"
	"github.com/go-tram/tram/internal/core/domain"
)

// ErrNoSignal is the reason a message is rejected whose signal is unknown:
// neither the source nor the delivery names it.
var ErrNoSignal = errors.New("message names no signal")

// Config describes a consumer.
type Config struct {
	Subscriber contract.Subscriber
	// Workers is the number of messages handled at once, at least one.
	Workers int
	// Signal is what every message holds. Empty, the delivery names it, see
	// contract.Signaled.
	Signal domain.Signal
	// Notify passes an event on, e.g. to the source's subscribers.
	Notify func(ctx context.Context, event *domain.Event) error
	// RetryDelay is how long to wait before a message whose delivery failed
	// is delivered again.
	RetryDelay func(d contract.Delivery) time.Duration
	Logger     *slog.Logger
}

// Consumer runs a subscriber until it is stopped.
type Consumer struct {
	cfg Config
	wg  sync.WaitGroup

	stop   context.Context
	cancel context.CancelFunc

	mu      sync.Mutex
	running bool
	stopped bool
}

func New(cfg Config) *Consumer {
	cfg.Workers = max(cfg.Workers, 1)
	stop, cancel := context.WithCancel(context.Background())
	return &Consumer{cfg: cfg, stop: stop, cancel: cancel}
}

// Run consumes until Stop. Deliveries use ctx, not the stop context, so
// messages being handled when the consumer stops are still ended.
func (c *Consumer) Run(ctx context.Context) error {
	c.mu.Lock()
	if c.stopped {
		c.mu.Unlock()
		return nil
	}
	c.running = true
	c.mu.Unlock()
	// The connection is closed once the workers are done, so messages still
	// being handled can be acknowledged.
	defer c.cfg.Subscriber.Close()

	if err := c.cfg.Subscriber.Open(ctx); err != nil {
		return err
	}
	c.consume(ctx)
	return nil
}

// consume takes a message only once a worker is free, so the driver buffers
// at most as many as there are workers.
func (c *Consumer) consume(ctx context.Context) {
	defer c.wg.Wait()
	slots := make(chan struct{}, c.cfg.Workers)
	for c.acquire(slots) {
		d, err := c.cfg.Subscriber.Next(c.stop)
		if err != nil {
			// Next returns errors only once the consumer stops.
			<-slots
			return
		}
		c.wg.Go(func() {
			defer func() { <-slots }()
			c.handle(ctx, d)
		})
	}
}

func (c *Consumer) acquire(slots chan struct{}) bool {
	// With a slot free, select would pick at random between it and a stop
	// that already happened, and receive again.
	if c.stop.Err() != nil {
		return false
	}
	select {
	case slots <- struct{}{}:
		return true
	case <-c.stop.Done():
		return false
	}
}

func (c *Consumer) handle(ctx context.Context, d contract.Delivery) {
	logger := c.cfg.Logger.With(slog.String("destination", d.Destination()))
	signal, err := c.signalOf(d)
	var event *domain.Event
	if err == nil {
		event, err = domain.UnmarshalOTLPData(signal, d.Body())
	}
	if err != nil {
		logger.WarnContext(ctx, "Rejected message that is not valid OTLP data",
			slog.String("signal", string(signal)), slog.Any("error", err))
		c.reject(ctx, logger, d)
		return
	}

	err = c.cfg.Notify(ctx, event)
	if errors.Is(err, domain.ErrEventRejected) {
		logger.WarnContext(ctx, "Rejected message", slog.Any("error", err))
		c.reject(ctx, logger, d)
		return
	}
	if err != nil {
		delay := c.cfg.RetryDelay(d)
		logger.WarnContext(ctx, "Could not deliver message, requesting redelivery",
			slog.Duration("delay", delay), slog.Any("error", err))
		if err = d.Retry(delay); err != nil {
			logger.ErrorContext(ctx, "Could not request redelivery of message", slog.Any("error", err))
		}
		return
	}

	if err = d.Ack(); err != nil {
		logger.ErrorContext(ctx, "Could not acknowledge message", slog.Any("error", err))
	}
}

func (c *Consumer) signalOf(d contract.Delivery) (domain.Signal, error) {
	if c.cfg.Signal != "" {
		return c.cfg.Signal, nil
	}
	if signaled, ok := d.(contract.Signaled); ok && signaled.Signal() != "" {
		return domain.Signal(signaled.Signal()), nil
	}
	return "", fmt.Errorf("%w; set the source's signal", ErrNoSignal)
}

func (c *Consumer) reject(ctx context.Context, logger *slog.Logger, d contract.Delivery) {
	if err := d.Reject(); err != nil {
		logger.ErrorContext(ctx, "Could not reject message", slog.Any("error", err))
	}
}

// Stop ends Run: no message is taken any more, and Run returns once the
// messages being handled are ended. A consumer that never ran closes its
// subscriber here.
func (c *Consumer) Stop() error {
	c.mu.Lock()
	c.stopped = true
	running := c.running
	c.mu.Unlock()

	c.cancel()
	if !running {
		return c.cfg.Subscriber.Close()
	}
	return nil
}

// Backoff returns a RetryDelay that doubles from initial up to maximum with
// every failed attempt, for deliveries that count them, see contract.Counted;
// others wait initial. The upper half of the delay is jittered, so messages
// that failed together spread out.
func Backoff(
	initial, maximum time.Duration,
	jitter func(time.Duration) time.Duration,
) func(contract.Delivery) time.Duration {
	return func(d contract.Delivery) time.Duration {
		attempts := 1
		if counted, ok := d.(contract.Counted); ok {
			attempts = counted.Attempts() + 1
		}
		delay := initial
		for i := 1; i < attempts && delay < maximum; i++ {
			delay *= 2
		}
		delay = min(delay, maximum)
		half := delay >> 1
		return half + jitter(delay-half)
	}
}
