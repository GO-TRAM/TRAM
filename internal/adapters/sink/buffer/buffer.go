// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

// Package buffer is the buffer sink: it stores every event in a queue in the
// data directory of the instance, through the driver its spec names, e.g.
// sqlite, and acknowledges only once the event is durable. A [buffer
// source](../source/buffer) of the same instance reads the queue and delivers
// it, with retries, to the rest of its stream.
package buffer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/go-playground/validator/v10"

	contract "github.com/go-tram/tram/internal/adapters/broker"
	"github.com/go-tram/tram/internal/adapters/buffer"
	"github.com/go-tram/tram/internal/adapters/buffer/drivers"
	"github.com/go-tram/tram/internal/adapters/driver"
	"github.com/go-tram/tram/internal/adapters/sink/base"
	"github.com/go-tram/tram/internal/core/domain"
)

const (
	Type = "buffer"

	defaultMaxPending = 256
	defaultMaxAge     = 24 * time.Hour
	defaultMaxBytes   = 1 << 30
)

// ErrInvalidSpec is the reason of a buffer spec that cannot be parsed.
var ErrInvalidSpec = errors.New("invalid buffer spec")

// Spec is the spec of a buffer sink. The driver's options are under the
// driver's name, e.g. `sqlite: {}`.
type Spec struct {
	// Driver selects the storage of the queue, e.g. sqlite.
	Driver string `json:"driver" validate:"required"`
	// Queue is the queue the events go to. The buffer source of the same
	// instance reads it.
	Queue string `json:"queue" validate:"required"`
	// MaxPending bounds the messages sent but not yet stored. Zero means
	// defaultMaxPending.
	MaxPending int `json:"max_pending,omitempty" validate:"omitempty,min=1"`
	// MaxAge is how long an entry waits for its delivery at most.
	MaxAge string `json:"max_age,omitempty"`
	// MaxBytes limits the size of the stored payloads.
	MaxBytes int64 `json:"max_bytes,omitempty" validate:"omitempty,min=1"`
}

// Sink stores events in a queue.
type Sink struct {
	*base.Sink[Spec]

	publisher contract.Publisher
	queue     string
	logger    *slog.Logger
}

// New creates the buffer sink with the driver its spec names, keeping the
// queue in dataDir, the data directory of the instance. Without one, New
// fails with buffer.ErrNoDataDir.
func New(sink domain.SinkSpec, validator *validator.Validate, logger *slog.Logger, dataDir string) (*Sink, error) {
	return newSink(sink, validator, logger, drivers.NewFactory(validator, dataDir))
}

// newSink is [New] with the drivers to resolve the spec's `driver` from, so
// tests can inject a driver.
func newSink(
	sink domain.SinkSpec,
	validator *validator.Validate,
	logger *slog.Logger,
	driverFactory driver.Factory[buffer.Driver],
) (*Sink, error) {
	generic, opts, drv, err := driver.Split(sink.Spec, driverFactory)
	if err != nil {
		return nil, err
	}
	b, err := base.New[Spec](sink.Name, sink.From, generic, validator)
	if err != nil {
		return nil, err
	}
	spec := b.Spec()

	maxAge := defaultMaxAge
	if spec.MaxAge != "" {
		if maxAge, err = time.ParseDuration(spec.MaxAge); err != nil {
			return nil, fmt.Errorf("%w: max_age: %w", ErrInvalidSpec, err)
		}
		if maxAge <= 0 {
			return nil, fmt.Errorf("%w: max_age %s is not positive", ErrInvalidSpec, spec.MaxAge)
		}
	}
	maxBytes := spec.MaxBytes
	if maxBytes == 0 {
		maxBytes = defaultMaxBytes
	}
	maxPending := spec.MaxPending
	if maxPending == 0 {
		maxPending = defaultMaxPending
	}

	logger = logger.With(slog.String("sink", sink.Name))
	publisher, err := drv.NewPublisher(opts, buffer.PublisherConfig{
		Name: "sink " + sink.Name, MaxPending: maxPending,
		Queue:    spec.Queue,
		MaxAge:   maxAge,
		MaxBytes: maxBytes,
	}, logger)
	if err != nil {
		return nil, err
	}
	return &Sink{Sink: b, publisher: publisher, queue: spec.Queue, logger: logger}, nil
}

// Notify stores the event, one message per signal, and returns once every
// message is durable. An event that can never fit is rejected; a full queue
// is a temporary error, which sources retry.
func (s *Sink) Notify(ctx context.Context, event *domain.Event) error {
	payloads, err := event.MarshalOTLPPayloads()
	if err != nil {
		return fmt.Errorf("%w: %w", domain.ErrEventRejected, err)
	}

	acks := make([]contract.Ack, 0, len(payloads))
	for _, p := range payloads {
		ack, publishErr := s.publisher.Publish(ctx, contract.Message{
			Destination: s.queue,
			Body:        p.Data,
			Signal:      string(p.Signal),
		})
		if publishErr != nil {
			err = publishErr
			break
		}
		acks = append(acks, ack)
	}
	for _, ack := range acks {
		if ackErr := ack.Wait(ctx); ackErr != nil {
			err = errors.Join(err, ackErr)
			if ctx.Err() != nil {
				return err
			}
		}
	}
	switch {
	case err == nil:
		return nil
	case errors.Is(err, contract.ErrMessageTooLarge):
		s.logger.WarnContext(ctx, "Message too large for the buffer, rejecting event", slog.Any("error", err))
		return fmt.Errorf("%w: %w", domain.ErrEventRejected, err)
	case errors.Is(err, buffer.ErrFull):
		return fmt.Errorf("buffer queue %s is full: %w", s.queue, err)
	default:
		return fmt.Errorf("store event in buffer queue %s: %w", s.queue, err)
	}
}

func (s *Sink) Stop() error {
	return s.publisher.Close()
}
