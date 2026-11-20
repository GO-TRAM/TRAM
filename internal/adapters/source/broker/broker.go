// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

// Package broker is the broker source: it receives OTLP data from a message
// broker through the driver its spec names. Every message is one serialized
// OTLP payload of the signal the spec names.
package broker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/go-playground/validator/v10"

	contract "codeberg.org/tram/tram/internal/adapters/broker"
	"codeberg.org/tram/tram/internal/adapters/broker/drivers"
	"codeberg.org/tram/tram/internal/adapters/broker/partition"
	"codeberg.org/tram/tram/internal/adapters/driver"
	"codeberg.org/tram/tram/internal/adapters/source/base"
	"codeberg.org/tram/tram/internal/core/domain"
)

const (
	Type = "broker"

	redeliveryDelay = 5 * time.Second
)

var ErrInvalidPartition = errors.New("invalid partition")

type Source struct {
	*base.Source[Spec]

	signal     domain.Signal
	subscriber contract.Subscriber
	logger     *slog.Logger
	wg         sync.WaitGroup

	stop   context.Context
	cancel context.CancelFunc

	mu      sync.Mutex
	running bool
	stopped bool
}

func New(source domain.SourceSpec, validator *validator.Validate, logger *slog.Logger) (*Source, error) {
	return newSource(source, validator, logger, drivers.NewSubscriberFactory(validator))
}

// newSource is [New] with the drivers to resolve the spec's `driver` from, so
// tests can inject the in-memory driver; see export_test.go.
func newSource(
	source domain.SourceSpec,
	validator *validator.Validate,
	logger *slog.Logger,
	driverFactory driver.Factory[contract.SubscriberDriver],
) (*Source, error) {
	generic, opts, drv, err := driver.Split(source.Spec, driverFactory)
	if err != nil {
		return nil, err
	}
	b, err := base.New[Spec](source.Name, generic, validator)
	if err != nil {
		return nil, err
	}
	if opts, err = expandPartition(b.Spec(), opts); err != nil {
		return nil, err
	}
	logger = logger.With(slog.String("source", source.Name))
	subscriber, err := drv.NewSubscriber(opts, contract.SubscriberConfig{
		Name:     "source " + source.Name,
		Capacity: workersOf(b.Spec()),
	}, logger)
	if err != nil {
		return nil, err
	}
	stop, cancel := context.WithCancel(context.Background())
	return &Source{
		Source:     b,
		signal:     domain.Signal(b.Spec().Signal),
		subscriber: subscriber,
		logger:     logger,
		stop:       stop,
		cancel:     cancel,
	}, nil
}

// expandPartition replaces {partition} in the driver's options with the
// partition of the key. Without partitioning the placeholder must not appear,
// and with it, it must: otherwise every key would read the same data.
func expandPartition(spec *Spec, opts map[string]any) (map[string]any, error) {
	p := spec.Partition
	part := 0
	if p != nil {
		part = partition.Of(p.Key, p.Count)
	}
	expanded, found := partition.ExpandAll(opts, part)
	switch {
	case p == nil && found:
		return nil, fmt.Errorf("%w: the driver's options contain %s, but partition is not set",
			ErrInvalidPartition, partition.Placeholder)
	case p != nil && !found:
		return nil, fmt.Errorf("%w: the driver's options need %s to read the key's partition",
			ErrInvalidPartition, partition.Placeholder)
	case p == nil:
		return opts, nil
	}
	out, _ := expanded.(map[string]any)
	return out, nil
}

func workersOf(spec *Spec) int {
	return int(max(spec.Workers, 1))
}

// Run consumes until Stop. Deliveries use ctx, not the stop context, so
// messages being handled when the source stops are still acknowledged.
func (s *Source) Run(ctx context.Context) error {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return nil
	}
	s.running = true
	s.mu.Unlock()
	// The connection is closed once the workers are done, so messages still
	// being handled can be acknowledged.
	defer s.subscriber.Close()

	if err := s.subscriber.Open(ctx); err != nil {
		return err
	}
	s.consume(ctx)
	return nil
}

// consume takes a message only once a worker is free, so the driver buffers
// at most as many as there are workers.
func (s *Source) consume(ctx context.Context) {
	defer s.wg.Wait()
	slots := make(chan struct{}, workersOf(s.Spec()))
	for s.acquire(slots) {
		d, err := s.subscriber.Next(s.stop)
		if err != nil {
			// Next returns errors only once the source stops.
			<-slots
			return
		}
		s.wg.Go(func() {
			defer func() { <-slots }()
			s.handle(ctx, d)
		})
	}
}

func (s *Source) acquire(slots chan struct{}) bool {
	// With a slot free, select would pick at random between it and a stop
	// that already happened, and receive again.
	if s.stop.Err() != nil {
		return false
	}
	select {
	case slots <- struct{}{}:
		return true
	case <-s.stop.Done():
		return false
	}
}

func (s *Source) handle(ctx context.Context, d contract.Delivery) {
	event, err := domain.UnmarshalOTLPData(s.signal, d.Body())
	if err != nil {
		s.logger.WarnContext(
			ctx,
			"Rejected message that is not valid OTLP data",
			slog.String(
				"signal",
				string(s.signal),
			),
			slog.String("destination", d.Destination()),
			slog.Any("error", err),
		)
		s.reject(ctx, d)
		return
	}

	err = s.NotifySubscribers(ctx, event)
	if errors.Is(err, domain.ErrEventRejected) {
		s.logger.WarnContext(ctx, "Rejected message", slog.Any("error", err))
		s.reject(ctx, d)
		return
	}
	if err != nil {
		s.logger.WarnContext(ctx, "Could not deliver message, requesting redelivery", slog.Any("error", err))
		if err = d.Retry(redeliveryDelay); err != nil {
			s.logger.ErrorContext(ctx, "Could not request redelivery of message", slog.Any("error", err))
		}
		return
	}

	if err = d.Ack(); err != nil {
		s.logger.ErrorContext(ctx, "Could not acknowledge message", slog.Any("error", err))
	}
}

func (s *Source) reject(ctx context.Context, d contract.Delivery) {
	if err := d.Reject(); err != nil {
		s.logger.ErrorContext(ctx, "Could not reject message", slog.Any("error", err))
	}
}

func (s *Source) Stop() error {
	s.mu.Lock()
	s.stopped = true
	running := s.running
	s.mu.Unlock()

	s.cancel()
	if !running {
		return s.subscriber.Close()
	}
	return nil
}
