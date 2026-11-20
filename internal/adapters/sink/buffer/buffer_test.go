// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package buffer_test

import (
	"context"
	"errors"
	"log/slog"
	"maps"
	"sync"
	"testing"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/go-tram/tram/internal/adapters/broker"
	"github.com/go-tram/tram/internal/adapters/buffer"
	"github.com/go-tram/tram/internal/adapters/driver"
	sinkbuffer "github.com/go-tram/tram/internal/adapters/sink/buffer"
	"github.com/go-tram/tram/internal/core/domain"
	"github.com/go-tram/tram/internal/testutil/otlpgen"
)

// fakeDriver serves an in-memory publisher that records the messages.
type fakeDriver struct {
	publisher *fakePublisher
	err       error
}

func (d *fakeDriver) NewPublisher(
	_ map[string]any,
	cfg buffer.PublisherConfig,
	_ *slog.Logger,
) (broker.Publisher, error) {
	if d.err != nil {
		return nil, d.err
	}
	d.publisher = &fakePublisher{queue: cfg.Queue, maxBytes: cfg.MaxBytes, maxAge: cfg.MaxAge}
	return d.publisher, nil
}

func (d *fakeDriver) NewSubscriber(
	_ map[string]any,
	_ buffer.SubscriberConfig,
	_ *slog.Logger,
) (broker.Subscriber, error) {
	return nil, errors.New("not needed in sink tests")
}

func (d *fakeDriver) Files(context.Context) ([]buffer.File, error) { return nil, nil }

type fakePublisher struct {
	queue    string
	maxBytes int64
	maxAge   time.Duration

	mu       sync.Mutex
	messages []broker.Message
	// failWith is returned by Publish when set.
	failWith error
}

func (p *fakePublisher) CheckDestination(string) error { return nil }
func (p *fakePublisher) MaxMessageSize() int {
	if p.maxBytes <= 0 {
		return 0
	}
	return int(p.maxBytes)
}

func (p *fakePublisher) Publish(_ context.Context, m broker.Message) (broker.Ack, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.failWith != nil {
		return nil, p.failWith
	}
	if p.maxBytes > 0 && int64(len(m.Body)) > p.maxBytes {
		return nil, broker.ErrMessageTooLarge
	}
	p.messages = append(p.messages, m)
	return fakeAck{}, nil
}

func (p *fakePublisher) Close() error { return nil }

func (p *fakePublisher) recorded() []broker.Message {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]broker.Message(nil), p.messages...)
}

type fakeAck struct{}

func (fakeAck) Wait(context.Context) error { return nil }

func testLogger() *slog.Logger { return slog.New(slog.DiscardHandler) }

func specOf(extra map[string]any) map[string]any {
	base := map[string]any{
		"driver": "fake",
		"fake":   map[string]any{},
		"queue":  "outbox",
	}
	maps.Copy(base, extra)
	return base
}

func newSinkForTest(spec domain.SinkSpec, factory driver.Factory[buffer.Driver]) (*sinkbuffer.Sink, error) {
	return sinkbuffer.NewWithDrivers(spec, validator.New(), testLogger(), factory)
}

func TestSpecValidation(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		spec    map[string]any
		wantErr error
	}{
		{name: "missing driver", spec: map[string]any{"queue": "q"}, wantErr: nil}, // checked below by message
		{name: "unknown driver", spec: specOf(map[string]any{"driver": "nope"}), wantErr: driver.ErrUnknown},
		{name: "missing queue", spec: map[string]any{"driver": "fake", "fake": map[string]any{}}},
		{name: "driver options under another name", spec: map[string]any{
			"driver": "fake", "queue": "q", "sqlite": map[string]any{},
		}},
		{name: "invalid max_age", spec: specOf(map[string]any{"max_age": "soon"}), wantErr: sinkbuffer.ErrInvalidSpec},
		{name: "negative max_age", spec: specOf(map[string]any{"max_age": "-1h"}), wantErr: sinkbuffer.ErrInvalidSpec},
		{name: "zero max_bytes", spec: specOf(map[string]any{"max_bytes": float64(0)})},
		{name: "valid with defaults", spec: specOf(nil)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			drv := &fakeDriver{}
			factory := driver.Set[buffer.Driver]{"fake": drv}
			s, err := newSinkForTest(domain.SinkSpec{Name: "buf", From: "in", Spec: tc.spec}, factory)
			switch {
			case tc.wantErr != nil:
				require.ErrorIs(t, err, tc.wantErr)
			case tc.name == "missing driver":
				require.Error(t, err)
				assert.Contains(t, err.Error(), "driver")
			case tc.name == "missing queue" || tc.name == "driver options under another name":
				require.Error(t, err)
			default:
				require.NoError(t, err)
				require.NoError(t, s.Stop())
			}
		})
	}
}

func TestNoDataDir(t *testing.T) {
	t.Parallel()
	_, err := sinkbuffer.New(domain.SinkSpec{
		Name: "buf", From: "in",
		Spec: map[string]any{"driver": "sqlite", "sqlite": map[string]any{}, "queue": "q"},
	}, validator.New(), testLogger(), "")
	require.ErrorIs(t, err, buffer.ErrNoDataDir)
}

// Notify stores one message per signal of the event, and only returns once
// the driver stored them.
func TestNotifyStoresOneMessagePerSignal(t *testing.T) {
	t.Parallel()

	drv := &fakeDriver{}
	s, err := newSinkForTest(domain.SinkSpec{
		Name: "buf", From: "in",
		Spec: specOf(map[string]any{"max_bytes": float64(1 << 20)}),
	}, driver.Set[buffer.Driver]{"fake": drv})
	require.NoError(t, err)
	defer s.Stop()

	shape := otlpgen.Shapes()[0]
	event := &domain.Event{Logs: otlpgen.Logs(shape), Traces: otlpgen.Traces(shape)}
	require.NoError(t, s.Notify(t.Context(), event))

	msgs := drv.publisher.recorded()
	require.Len(t, msgs, 2)
	bySignal := map[string]broker.Message{}
	for _, m := range msgs {
		assert.Equal(t, "outbox", m.Destination)
		bySignal[m.Signal] = m
	}
	require.Contains(t, bySignal, "logs")
	require.Contains(t, bySignal, "traces")

	logs, err := domain.UnmarshalOTLPData(domain.SignalLogs, bySignal["logs"].Body)
	require.NoError(t, err)
	assert.Len(t, logs.Logs.GetResourceLogs(), len(event.Logs.GetResourceLogs()))
}

// A message that can never fit into the queue rejects the event for good.
func TestNotifyTooLargeRejectsEvent(t *testing.T) {
	t.Parallel()

	drv := &fakeDriver{}
	s, err := newSinkForTest(domain.SinkSpec{
		Name: "buf", From: "in",
		Spec: specOf(map[string]any{"max_bytes": float64(8)}),
	}, driver.Set[buffer.Driver]{"fake": drv})
	require.NoError(t, err)
	defer s.Stop()

	err = s.Notify(t.Context(), &domain.Event{Logs: otlpgen.Logs(otlpgen.Shapes()[0])})
	require.ErrorIs(t, err, domain.ErrEventRejected)
	assert.Empty(t, drv.publisher.recorded())
}

// A full queue is a temporary failure: the source retries the event later.
func TestNotifyFullIsRetryable(t *testing.T) {
	t.Parallel()

	drv := &fakeDriver{}
	drv.err = nil
	s, err := newSinkForTest(domain.SinkSpec{
		Name: "buf", From: "in", Spec: specOf(nil),
	}, driver.Set[buffer.Driver]{"fake": drv})
	require.NoError(t, err)
	defer s.Stop()
	drv.publisher.failWith = buffer.ErrFull

	err = s.Notify(t.Context(), &domain.Event{Logs: otlpgen.Logs(otlpgen.Shapes()[0])})
	require.Error(t, err)
	require.NotErrorIs(t, err, domain.ErrEventRejected)
	assert.ErrorIs(t, err, buffer.ErrFull)
}
