// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

// Package buffer is the buffer source: it reads a queue from the data
// directory of the instance, through the driver its spec names, e.g. sqlite,
// and delivers every message to its stream. A [buffer sink](../sink/buffer)
// of the same instance writes the queue. Deliveries that fail are retried
// with exponential backoff; entries older than the queue's max_age are
// removed.
package buffer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/go-playground/validator/v10"

	"github.com/go-tram/tram/internal/adapters/broker/consume"
	"github.com/go-tram/tram/internal/adapters/buffer"
	"github.com/go-tram/tram/internal/adapters/buffer/drivers"
	"github.com/go-tram/tram/internal/adapters/driver"
	"github.com/go-tram/tram/internal/adapters/source/base"
	"github.com/go-tram/tram/internal/core/domain"
)

const (
	Type = "buffer"

	defaultRetryInitial = time.Second
	defaultRetryMax     = 5 * time.Minute
)

// ErrInvalidSpec is the reason of a buffer spec that cannot be parsed.
var ErrInvalidSpec = errors.New("invalid buffer spec")

// Spec is the spec of a buffer source. The driver's options are under the
// driver's name, e.g. `sqlite: {}`.
type Spec struct {
	// Driver selects the storage of the queue, e.g. sqlite.
	Driver string `json:"driver" validate:"required"`
	// Queue is the queue the messages come from. The buffer sink of the same
	// instance writes it.
	Queue string `json:"queue" validate:"required"`
	// Signal is what every message holds, if the driver's deliveries do not
	// name it. The sqlite driver names it.
	Signal string `json:"signal,omitempty" validate:"omitempty,oneof=logs metrics traces"`
	// Workers bounds how many messages are handled at once. Zero means one.
	Workers uint `json:"workers,omitempty" validate:"max=10000"`
	// RetryInitial is the delay before the first retry of a failed delivery;
	// it doubles with every failure.
	RetryInitial string `json:"retry_initial,omitempty"`
	// RetryMax is the longest delay between retries.
	RetryMax string `json:"retry_max,omitempty"`
}

// Source reads a queue and delivers its messages.
type Source struct {
	*base.Source[Spec]

	consumer *consume.Consumer
}

// New creates the buffer source with the driver its spec names, reading the
// queue from dataDir, the data directory of the instance. Without one, New
// fails with buffer.ErrNoDataDir.
func New(
	source domain.SourceSpec,
	validator *validator.Validate,
	logger *slog.Logger,
	dataDir string,
) (*Source, error) {
	return newSource(source, validator, logger, drivers.NewFactory(validator, dataDir))
}

// newSource is [New] with the drivers to resolve the spec's `driver` from, so
// tests can inject a driver.
func newSource(
	source domain.SourceSpec,
	validator *validator.Validate,
	logger *slog.Logger,
	driverFactory driver.Factory[buffer.Driver],
) (*Source, error) {
	generic, opts, drv, err := driver.Split(source.Spec, driverFactory)
	if err != nil {
		return nil, err
	}
	b, err := base.New[Spec](source.Name, generic, validator)
	if err != nil {
		return nil, err
	}
	spec := b.Spec()

	initial, maxDelay, err := retryBounds(spec)
	if err != nil {
		return nil, err
	}

	logger = logger.With(slog.String("source", source.Name))
	subscriber, err := drv.NewSubscriber(opts, buffer.SubscriberConfig{
		Name:     "source " + source.Name,
		Capacity: workersOf(spec),
		Queue:    spec.Queue,
	}, logger)
	if err != nil {
		return nil, err
	}
	s := &Source{Source: b}
	s.consumer = consume.New(consume.Config{
		Subscriber: subscriber,
		Workers:    workersOf(spec),
		Signal:     domain.Signal(spec.Signal),
		Notify:     s.NotifySubscribers,
		RetryDelay: consume.Backoff(initial, maxDelay, jitter),
		Logger:     logger,
	})
	return s, nil
}

func retryBounds(spec *Spec) (time.Duration, time.Duration, error) {
	initial := defaultRetryInitial
	if spec.RetryInitial != "" {
		parsed, err := time.ParseDuration(spec.RetryInitial)
		if err != nil || parsed <= 0 {
			return 0, 0, fmt.Errorf("%w: retry_initial %q", ErrInvalidSpec, spec.RetryInitial)
		}
		initial = parsed
	}
	maxDelay := defaultRetryMax
	if spec.RetryMax != "" {
		parsed, err := time.ParseDuration(spec.RetryMax)
		if err != nil || parsed <= 0 {
			return 0, 0, fmt.Errorf("%w: retry_max %q", ErrInvalidSpec, spec.RetryMax)
		}
		maxDelay = parsed
	}
	if initial > maxDelay {
		return 0, 0, fmt.Errorf("%w: retry_initial %s is above retry_max %s", ErrInvalidSpec, initial, maxDelay)
	}
	return initial, maxDelay, nil
}

// jitter spreads the upper half of a retry delay.
func jitter(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	return rand.N(d) //nolint:gosec // Jitter, not a secret.
}

// Run consumes until Stop. Deliveries use ctx, not the stop context, so
// messages being handled when the source stops are still ended.
func (s *Source) Run(ctx context.Context) error { return s.consumer.Run(ctx) }

func (s *Source) Stop() error { return s.consumer.Stop() }

func workersOf(spec *Spec) int { return int(max(spec.Workers, 1)) }
