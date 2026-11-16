// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

// Package cloudevent sends events as CloudEvents through a message broker
// chosen by configuration.
package cloudevent

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/cloudevents/sdk-go/v2/binding"
	"github.com/go-playground/validator/v10"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	tracespb "go.opentelemetry.io/proto/otlp/trace/v1"

	"codeberg.org/eljoth/tram/internal/adapters/cebroker"
	"codeberg.org/eljoth/tram/internal/adapters/sink/base"
	"codeberg.org/eljoth/tram/internal/adapters/sink/celentry"
	"codeberg.org/eljoth/tram/internal/core/domain"
)

const (
	Type = "cloudevent"

	// Sends at once per sink, like the async publish window of the NATS sink:
	// the protocol bindings send one event at a time and wait for the broker.
	defaultConcurrency = 256
)

type CloudEvent struct {
	*base.Sink[Spec]

	destinations celentry.Programs
	source       string
	sender       cebroker.Sender
	// slots bounds the events in flight across all calls of Notify.
	slots  chan struct{}
	logger *slog.Logger
}

func New(sink domain.SinkSpec, validator *validator.Validate, logger *slog.Logger) (*CloudEvent, error) {
	b, err := base.New[Spec](sink.Name, sink.From, sink.Spec, validator)
	if err != nil {
		return nil, err
	}
	c, err := newWithoutSender(b, logger)
	if err != nil {
		return nil, err
	}

	switch b.Spec().Broker {
	case cebroker.BrokerNATS:
		c.sender, err = cebroker.NewNATSSender(b.Spec().NATS, "sink "+sink.Name,
			logger.With(slog.String("sink", sink.Name)))
	default:
		err = fmt.Errorf("unsupported broker %q", b.Spec().Broker)
	}
	if err != nil {
		return nil, err
	}
	return c, nil
}

func newWithoutSender(b *base.Sink[Spec], logger *slog.Logger) (*CloudEvent, error) {
	spec := b.Spec()
	destinations, err := celentry.Compile(celentry.Expressions{
		Logs: spec.Destination.Logs, Metrics: spec.Destination.Metrics, Traces: spec.Destination.Traces,
	})
	if err != nil {
		return nil, err
	}
	source := spec.Source
	if source == "" {
		source = "tram/" + b.Name()
	}
	concurrency := spec.Concurrency
	if concurrency == 0 {
		concurrency = defaultConcurrency
	}
	return &CloudEvent{
		Sink:         b,
		destinations: destinations,
		source:       source,
		slots:        make(chan struct{}, concurrency),
		logger:       logger,
	}, nil
}

type message struct {
	destination string
	event       *domain.Event
}

func (c *CloudEvent) Notify(ctx context.Context, event *domain.Event) error {
	messages, err := c.group(event)
	if err != nil {
		// Resolving the destinations of the same event fails the same way
		// again, so the event is rejected instead of retried.
		c.logger.WarnContext(ctx, "Could not resolve destination, rejecting event",
			slog.String("sink", c.Name()), slog.Any("error", err))
		return fmt.Errorf("%w: could not resolve destination: %w", domain.ErrEventRejected, err)
	}
	if err = c.send(ctx, messages); err != nil {
		return fmt.Errorf("failed to send event: %w", err)
	}
	return nil
}

// send sends every message as its own CloudEvent, as many at once as the sink
// has free slots, and returns once all are stored.
func (c *CloudEvent) send(ctx context.Context, messages []message) error {
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		errs error
	)
	// Most events go to one destination; it is sent without a goroutine.
	if len(messages) == 1 {
		select {
		case c.slots <- struct{}{}:
		case <-ctx.Done():
			return ctx.Err()
		}
		defer func() { <-c.slots }()
		return c.sendOne(ctx, messages[0])
	}
	for _, m := range messages {
		select {
		case c.slots <- struct{}{}:
		case <-ctx.Done():
			wg.Wait()
			return errors.Join(errs, ctx.Err())
		}
		wg.Go(func() {
			defer func() { <-c.slots }()
			if err := c.sendOne(ctx, m); err != nil {
				mu.Lock()
				errs = errors.Join(errs, err)
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	return errs
}

func (c *CloudEvent) sendOne(ctx context.Context, m message) error {
	signal, data, err := m.event.MarshalOTLPData()
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}
	e := cebroker.NewEvent(c.source, m.destination, signal, data)
	return c.sender.Send(ctx, m.destination, binding.ToMessage(e))
}

// group splits the event into one message per destination and signal.
func (c *CloudEvent) group(event *domain.Event) ([]message, error) {
	var messages []message
	resolve := celentry.String
	spec := c.Spec().Destination

	if event.Logs != nil && spec.Logs != "" {
		parts, err := domain.PartitionLogs(event.Logs, celentry.LogKey(c.destinations.Logs, resolve))
		if err != nil {
			return nil, fmt.Errorf("logs: %w", err)
		}
		messages = appendMessages(messages, parts, func(d *logspb.LogsData) *domain.Event {
			return &domain.Event{Logs: d}
		})
	}
	if event.Metrics != nil && spec.Metrics != "" {
		parts, err := domain.PartitionMetrics(event.Metrics, celentry.MetricKey(c.destinations.Metrics, resolve))
		if err != nil {
			return nil, fmt.Errorf("metrics: %w", err)
		}
		messages = appendMessages(messages, parts, func(d *metricspb.MetricsData) *domain.Event {
			return &domain.Event{Metrics: d}
		})
	}
	if event.Traces != nil && spec.Traces != "" {
		parts, err := domain.PartitionSpans(event.Traces, celentry.SpanKey(c.destinations.Traces, resolve))
		if err != nil {
			return nil, fmt.Errorf("traces: %w", err)
		}
		messages = appendMessages(messages, parts, func(d *tracespb.TracesData) *domain.Event {
			return &domain.Event{Traces: d}
		})
	}
	// Checked once per destination rather than for every record.
	for _, m := range messages {
		if err := c.sender.CheckDestination(m.destination); err != nil {
			return nil, err
		}
	}
	return messages, nil
}

func appendMessages[D any](messages []message, parts []domain.Part[D], event func(D) *domain.Event) []message {
	for _, part := range parts {
		messages = append(messages, message{destination: part.Key, event: event(part.Data)})
	}
	return messages
}

func (c *CloudEvent) Stop() error {
	return c.sender.Close()
}
