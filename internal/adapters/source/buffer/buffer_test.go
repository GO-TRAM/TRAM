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
	sourcebuffer "github.com/go-tram/tram/internal/adapters/source/buffer"
	"github.com/go-tram/tram/internal/core/domain"
	"github.com/go-tram/tram/internal/testutil/otlpgen"
)

func testLogger() *slog.Logger { return slog.New(slog.DiscardHandler) }

// fakeDriver serves a subscriber the test fills with deliveries.
type fakeDriver struct {
	subscriber *fakeSubscriber
	err        error
}

func (d *fakeDriver) NewPublisher(
	_ map[string]any,
	_ buffer.PublisherConfig,
	_ *slog.Logger,
) (broker.Publisher, error) {
	return nil, errors.New("not needed in source tests")
}

func (d *fakeDriver) NewSubscriber(
	_ map[string]any,
	cfg buffer.SubscriberConfig,
	_ *slog.Logger,
) (broker.Subscriber, error) {
	if d.err != nil {
		return nil, d.err
	}
	d.subscriber = &fakeSubscriber{queue: cfg.Queue, capacity: cfg.Capacity, ch: make(chan broker.Delivery, 16)}
	return d.subscriber, nil
}

func (d *fakeDriver) Files(context.Context) ([]buffer.File, error) { return nil, nil }

type fakeSubscriber struct {
	queue    string
	capacity int

	mu     sync.Mutex
	ch     chan broker.Delivery
	closed bool
}

func (s *fakeSubscriber) Open(context.Context) error { return nil }

func (s *fakeSubscriber) Next(ctx context.Context) (broker.Delivery, error) {
	select {
	case d := <-s.ch:
		return d, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *fakeSubscriber) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

func (s *fakeSubscriber) offer(d broker.Delivery) { s.ch <- d }

// fakeDelivery is one message with an end the test watches.
type fakeDelivery struct {
	body     []byte
	signal   string
	attempts int

	mu       sync.Mutex
	acked    bool
	rejected bool
	retried  time.Duration
}

var (
	_ broker.Delivery = (*fakeDelivery)(nil)
	_ broker.Signaled = (*fakeDelivery)(nil)
	_ broker.Counted  = (*fakeDelivery)(nil)
)

func (d *fakeDelivery) Body() []byte        { return d.body }
func (d *fakeDelivery) Destination() string { return "outbox" }
func (d *fakeDelivery) Signal() string      { return d.signal }
func (d *fakeDelivery) Attempts() int       { return d.attempts }

func (d *fakeDelivery) Ack() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.acked = true
	return nil
}

func (d *fakeDelivery) Retry(delay time.Duration) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.retried = delay
	return nil
}

func (d *fakeDelivery) Reject() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.rejected = true
	return nil
}

// state returns whether the delivery was acknowledged or rejected, and the
// delay of its retry.
func (d *fakeDelivery) state() (bool, bool, time.Duration) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.acked, d.rejected, d.retried
}

func sourceSpec(extra map[string]any) domain.SourceSpec {
	base := map[string]any{
		"driver": "fake",
		"fake":   map[string]any{},
		"queue":  "outbox",
	}
	maps.Copy(base, extra)
	return domain.SourceSpec{Name: "buf", Type: "buffer", Spec: base}
}

func newSourceForTest(spec domain.SourceSpec, factory driver.Factory[buffer.Driver]) (*sourcebuffer.Source, error) {
	return sourcebuffer.NewWithDrivers(spec, validator.New(), testLogger(), factory)
}

func TestSourceSpecValidation(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		spec    domain.SourceSpec
		wantErr error
	}{
		{name: "unknown driver", spec: sourceSpec(map[string]any{"driver": "nope"}), wantErr: driver.ErrUnknown},
		{name: "missing queue", spec: domain.SourceSpec{Name: "buf", Type: "buffer",
			Spec: map[string]any{"driver": "fake", "fake": map[string]any{}}}},
		{name: "invalid signal", spec: sourceSpec(map[string]any{"signal": "spans"})},
		{name: "invalid retry_initial", spec: sourceSpec(map[string]any{"retry_initial": "later"}),
			wantErr: sourcebuffer.ErrInvalidSpec},
		{name: "negative retry_max", spec: sourceSpec(map[string]any{"retry_max": "-5s"}),
			wantErr: sourcebuffer.ErrInvalidSpec},
		{name: "retry_initial above retry_max", spec: sourceSpec(map[string]any{
			"retry_initial": "10s", "retry_max": "5s"}), wantErr: sourcebuffer.ErrInvalidSpec},
		{name: "valid with defaults", spec: sourceSpec(nil)},
		{name: "valid with retry bounds", spec: sourceSpec(map[string]any{
			"retry_initial": "10ms", "retry_max": "1s", "workers": float64(4), "signal": "logs"})},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := newSourceForTest(tc.spec, driver.Set[buffer.Driver]{"fake": &fakeDriver{}})
			switch {
			case tc.wantErr != nil:
				require.ErrorIs(t, err, tc.wantErr)
			case tc.name == "missing queue" || tc.name == "invalid signal":
				require.Error(t, err)
			default:
				require.NoError(t, err)
			}
		})
	}
}

func TestNoDataDir(t *testing.T) {
	t.Parallel()
	_, err := sourcebuffer.New(domain.SourceSpec{
		Name: "buf", Type: "buffer",
		Spec: map[string]any{"driver": "sqlite", "sqlite": map[string]any{}, "queue": "q"},
	}, validator.New(), testLogger(), "")
	require.ErrorIs(t, err, buffer.ErrNoDataDir)
}

// recorder is a subscriber of the source under test.
type recorder struct {
	mu     sync.Mutex
	events []*domain.Event
	err    error
}

func (r *recorder) Name() string { return "recorder" }
func (r *recorder) From() string { return "buf" }
func (r *recorder) Stop() error  { return nil }

func (r *recorder) Notify(_ context.Context, e *domain.Event) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return r.err
	}
	r.events = append(r.events, e)
	return nil
}

func (r *recorder) received() []*domain.Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]*domain.Event(nil), r.events...)
}

func logsBody(t *testing.T) []byte {
	t.Helper()
	_, data, err := (&domain.Event{Logs: otlpgen.Logs(otlpgen.Shapes()[0])}).MarshalOTLPData()
	require.NoError(t, err)
	return data
}

// A stored message is delivered to the stream as an event and acknowledged.
func TestSourceDeliversAndAcks(t *testing.T) {
	t.Parallel()

	drv := &fakeDriver{}
	s, err := newSourceForTest(
		sourceSpec(map[string]any{"retry_initial": "1ms"}),
		driver.Set[buffer.Driver]{"fake": drv},
	)
	require.NoError(t, err)

	rec := &recorder{}
	require.NoError(t, s.Subscribe(rec))

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	defer func() { require.NoError(t, s.Stop()) }()

	d := &fakeDelivery{body: logsBody(t), signal: "logs"}
	drv.subscriber.offer(d)

	require.Eventually(t, func() bool { return len(rec.received()) == 1 }, 5*time.Second, time.Millisecond)
	require.Eventually(t, func() bool {
		acked, rejected, _ := d.state()
		return acked && !rejected
	}, 5*time.Second, time.Millisecond)

	got := rec.received()[0]
	assert.NotNil(t, got.Logs)
	assert.Len(t, got.Logs.GetResourceLogs(), 1)
}

// A message the stream rejects for good is rejected, not retried.
func TestSourceRejectsWhatTheStreamRejects(t *testing.T) {
	t.Parallel()

	drv := &fakeDriver{}
	s, err := newSourceForTest(
		sourceSpec(map[string]any{"retry_initial": "1ms"}),
		driver.Set[buffer.Driver]{"fake": drv},
	)
	require.NoError(t, err)

	rec := &recorder{err: domain.ErrEventRejected}
	require.NoError(t, s.Subscribe(rec))

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() { _ = s.Run(ctx) }()
	defer func() { require.NoError(t, s.Stop()) }()

	d := &fakeDelivery{body: logsBody(t), signal: "logs"}
	drv.subscriber.offer(d)

	require.Eventually(t, func() bool {
		_, rejected, retried := d.state()
		return rejected && retried == 0
	}, 5*time.Second, time.Millisecond)
}

// A message whose delivery fails is retried with a backoff, not rejected.
func TestSourceRetriesFailedDelivery(t *testing.T) {
	t.Parallel()

	drv := &fakeDriver{}
	s, err := newSourceForTest(sourceSpec(map[string]any{"retry_initial": "1ms", "retry_max": "10ms"}),
		driver.Set[buffer.Driver]{"fake": drv})
	require.NoError(t, err)

	rec := &recorder{err: errors.New("sink unavailable")}
	require.NoError(t, s.Subscribe(rec))

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() { _ = s.Run(ctx) }()
	defer func() { require.NoError(t, s.Stop()) }()

	d := &fakeDelivery{body: logsBody(t), signal: "logs", attempts: 2}
	drv.subscriber.offer(d)

	require.Eventually(t, func() bool {
		_, rejected, retried := d.state()
		return !rejected && retried >= time.Millisecond
	}, 5*time.Second, time.Millisecond)
	// Two failed attempts with initial 1ms double to 4ms.
	_, _, retried := d.state()
	assert.LessOrEqual(t, retried, 10*time.Millisecond)
}

// A message whose signal neither the source nor the delivery names is
// rejected: it could not be turned into an event.
func TestSourceRejectsMessageWithoutSignal(t *testing.T) {
	t.Parallel()

	drv := &fakeDriver{}
	// No signal in the spec, and the delivery names none.
	s, err := newSourceForTest(
		sourceSpec(map[string]any{"retry_initial": "1ms"}),
		driver.Set[buffer.Driver]{"fake": drv},
	)
	require.NoError(t, err)

	rec := &recorder{}
	require.NoError(t, s.Subscribe(rec))

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() { _ = s.Run(ctx) }()
	defer func() { require.NoError(t, s.Stop()) }()

	d := &fakeDelivery{body: logsBody(t), signal: ""}
	drv.subscriber.offer(d)

	require.Eventually(t, func() bool {
		acked, rejected, _ := d.state()
		return rejected && !acked
	}, 5*time.Second, time.Millisecond)
	assert.Empty(t, rec.received())
}
