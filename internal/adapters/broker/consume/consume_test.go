// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package consume_test

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/go-tram/tram/internal/adapters/broker"
	"github.com/go-tram/tram/internal/adapters/broker/consume"
	"github.com/go-tram/tram/internal/core/domain"
	"github.com/go-tram/tram/internal/testutil/otlpgen"
)

func testLogger() *slog.Logger { return slog.New(slog.DiscardHandler) }

type fakeDelivery struct {
	body     string
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

func (d *fakeDelivery) Body() []byte        { return []byte(d.body) }
func (d *fakeDelivery) Destination() string { return "dest" }
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

// plainDelivery names no signal and counts no attempts.
type plainDelivery struct{ fakeDelivery }

type fakeSubscriber struct {
	ch chan broker.Delivery
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

func (s *fakeSubscriber) Close() error { return nil }

// TestBackoff doubles the delay with every failed attempt, caps it at the
// maximum, and jitters only the upper half, so the lower half is a floor.
func TestBackoff(t *testing.T) {
	t.Parallel()

	identity := func(d time.Duration) time.Duration { return d }
	backoff := consume.Backoff(time.Second, 8*time.Second, identity)

	cases := []struct {
		attempts int
		want     time.Duration
	}{
		{attempts: 0, want: time.Second},
		{attempts: 1, want: 2 * time.Second},
		{attempts: 2, want: 4 * time.Second},
		{attempts: 3, want: 8 * time.Second},
		{attempts: 10, want: 8 * time.Second}, // capped
	}
	for _, tc := range cases {
		d := &fakeDelivery{attempts: tc.attempts}
		assert.Equal(t, tc.want, backoff(d), "attempts %d", tc.attempts)
	}

	// Without attempts, the first retry waits the initial delay.
	assert.Equal(t, time.Second, backoff(&plainDelivery{}))

	// Jitter only spreads the upper half: with zero jitter the lower half
	// remains.
	zero := consume.Backoff(10*time.Second, time.Minute, func(time.Duration) time.Duration { return 0 })
	assert.Equal(t, 5*time.Second, zero(&fakeDelivery{}))
}

// runConsumer starts one consumer over the deliveries offered, passing
// events to notify, and stops it when the test ends.
func runConsumer(
	t *testing.T,
	sub *fakeSubscriber,
	notify func(ctx context.Context, event *domain.Event) error,
) {
	t.Helper()
	c := consume.New(consume.Config{
		Subscriber: sub,
		Workers:    1,
		Signal:     domain.SignalLogs,
		Notify:     notify,
		RetryDelay: func(broker.Delivery) time.Duration { return time.Millisecond },
		Logger:     testLogger(),
	})
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	t.Cleanup(func() {
		require.NoError(t, c.Stop())
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("Run did not return after Stop")
		}
	})
}

func TestConsumerAcksDeliveredMessages(t *testing.T) {
	t.Parallel()

	sub := &fakeSubscriber{ch: make(chan broker.Delivery, 1)}
	var got *domain.Event
	runConsumer(t, sub, func(_ context.Context, e *domain.Event) error {
		got = e
		return nil
	})

	body := logsBody(t)
	d := &fakeDelivery{body: string(body), signal: "logs"}
	sub.ch <- d

	require.Eventually(t, func() bool {
		acked, rejected, _ := d.state()
		return acked && !rejected
	}, 5*time.Second, time.Millisecond)
	require.NotNil(t, got)
	assert.NotNil(t, got.Logs)
}

func TestConsumerRejectsRejectedEvents(t *testing.T) {
	t.Parallel()

	sub := &fakeSubscriber{ch: make(chan broker.Delivery, 1)}
	runConsumer(t, sub, func(context.Context, *domain.Event) error {
		return domain.ErrEventRejected
	})

	d := &fakeDelivery{body: string(logsBody(t)), signal: "logs"}
	sub.ch <- d

	require.Eventually(t, func() bool {
		_, rejected, retried := d.state()
		return rejected && retried == 0
	}, 5*time.Second, time.Millisecond)
}

func TestConsumerRetriesFailedDeliveries(t *testing.T) {
	t.Parallel()

	sub := &fakeSubscriber{ch: make(chan broker.Delivery, 1)}
	runConsumer(t, sub, func(context.Context, *domain.Event) error {
		return errors.New("sink unavailable")
	})

	d := &fakeDelivery{body: string(logsBody(t)), signal: "logs"}
	sub.ch <- d

	require.Eventually(t, func() bool {
		acked, rejected, retried := d.state()
		return !acked && !rejected && retried == time.Millisecond
	}, 5*time.Second, time.Millisecond)
}

// A message whose signal neither the config nor the delivery names is
// rejected: it cannot be unmarshalled.
func TestConsumerRejectsMessageWithoutSignal(t *testing.T) {
	t.Parallel()

	sub := &fakeSubscriber{ch: make(chan broker.Delivery, 1)}
	c := consume.New(consume.Config{
		Subscriber: sub,
		Workers:    1,
		Notify:     func(context.Context, *domain.Event) error { return nil },
		RetryDelay: func(broker.Delivery) time.Duration { return time.Millisecond },
		Logger:     testLogger(),
	})
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	go func() { _ = c.Run(ctx) }()
	t.Cleanup(func() { _ = c.Stop() })

	d := &plainDelivery{fakeDelivery{body: string(logsBody(t))}}
	sub.ch <- d

	require.Eventually(t, func() bool {
		acked, rejected, _ := d.state()
		return rejected && !acked
	}, 5*time.Second, time.Millisecond)
}

func logsBody(t *testing.T) []byte {
	t.Helper()
	event := &domain.Event{Logs: otlpgen.Logs(otlpgen.Shapes()[0])}
	_, data, err := event.MarshalOTLPData()
	require.NoError(t, err)
	return data
}
