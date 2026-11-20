// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package brokertest

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/go-tram/tram/internal/adapters/broker"
)

const (
	// quiet is how long a test waits to be sure nothing more arrives.
	quiet = time.Second
	// arrival is how long a test waits for a message that has to arrive.
	arrival = 10 * time.Second
	// retryDelay is short, so the suite stays fast, but long enough to tell
	// a delayed redelivery from an immediate one. A redelivery after at least
	// half of it counts as delayed, to allow for clock and scheduling slack.
	retryDelay    = 700 * time.Millisecond
	minRetryDelay = retryDelay / 2
	// settle lets Next start waiting before its context ends; stop is how
	// long it may take to return afterwards.
	settle = 100 * time.Millisecond
	stop   = 2 * time.Second
	// capacity of the subscriber that receives several messages at once.
	severalCapacity = 4
)

// Harness connects the suite to one driver.
type Harness struct {
	Driver broker.Driver
	// Setup prepares a fresh destination on the broker, e.g. a stream that
	// holds it, and returns the options of a publisher and of a durable
	// subscriber reading it.
	Setup func(t *testing.T) Setup
	// InvalidDestinations are destinations no message can be published to.
	InvalidDestinations []string
}

// Setup is one destination with the options to publish to and read it.
type Setup struct {
	Destination string
	Publisher   map[string]any
	Subscriber  map[string]any
}

// Run checks the behavior the broker sink and source rely on. Each case gets
// its own destination, so the cases run in parallel.
func Run(t *testing.T, h Harness) {
	t.Helper()
	cases := []struct {
		name string
		run  func(t *testing.T, h Harness, s Setup)
	}{
		{"published messages are received", testPublishReceive},
		{"acknowledged messages are not delivered again", testAck},
		{"retried messages come again after the delay", testRetry},
		{"rejected messages never come again", testReject},
		{"a new subscriber continues after the last acknowledgement", testDurable},
		{"next returns when its context ends", testNextCancel},
		{"invalid destinations are refused", testInvalidDestinations},
		{"messages above the maximum size are refused", testTooLarge},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			c.run(t, h, h.Setup(t))
		})
	}
}

func discard() *slog.Logger { return slog.New(slog.DiscardHandler) }

func publisher(t *testing.T, h Harness, s Setup) broker.Publisher {
	t.Helper()
	p, err := h.Driver.NewPublisher(
		s.Publisher,
		broker.PublisherConfig{Name: "conformance", MaxPending: severalCapacity},
		discard(),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = p.Close() })
	return p
}

func subscriber(t *testing.T, h Harness, s Setup, capacity int) broker.Subscriber {
	t.Helper()
	sub, err := h.Driver.NewSubscriber(
		s.Subscriber,
		broker.SubscriberConfig{Name: "conformance", Capacity: capacity},
		discard(),
	)
	require.NoError(t, err)
	require.NoError(t, sub.Open(t.Context()))
	t.Cleanup(func() { _ = sub.Close() })
	return sub
}

func publish(t *testing.T, p broker.Publisher, dest string, bodies ...string) {
	t.Helper()
	acks := make([]broker.Ack, 0, len(bodies))
	for _, body := range bodies {
		ack, err := p.Publish(t.Context(), broker.Message{Destination: dest, Body: []byte(body)})
		require.NoError(t, err)
		acks = append(acks, ack)
	}
	for _, ack := range acks {
		require.NoError(t, ack.Wait(t.Context()))
	}
}

func next(t *testing.T, sub broker.Subscriber, wait time.Duration) (broker.Delivery, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), wait)
	defer cancel()
	return sub.Next(ctx)
}

func requireNext(t *testing.T, sub broker.Subscriber) broker.Delivery {
	t.Helper()
	d, err := next(t, sub, arrival)
	require.NoError(t, err, "no message arrived")
	return d
}

func requireNothing(t *testing.T, sub broker.Subscriber, msg string) {
	t.Helper()
	d, err := next(t, sub, quiet)
	if err == nil {
		t.Fatalf("%s, but %q arrived", msg, d.Body())
	}
}

func testPublishReceive(t *testing.T, h Harness, s Setup) {
	p := publisher(t, h, s)
	sub := subscriber(t, h, s, severalCapacity)
	publish(t, p, s.Destination, "a", "b", "c")

	var got []string
	for range 3 {
		d := requireNext(t, sub)
		assert.Equal(t, s.Destination, d.Destination())
		got = append(got, string(d.Body()))
		require.NoError(t, d.Ack())
	}
	assert.ElementsMatch(t, []string{"a", "b", "c"}, got)
}

func testAck(t *testing.T, h Harness, s Setup) {
	p := publisher(t, h, s)
	sub := subscriber(t, h, s, 1)
	publish(t, p, s.Destination, "a")
	require.NoError(t, requireNext(t, sub).Ack())
	requireNothing(t, sub, "an acknowledged message must not come again")
}

func testRetry(t *testing.T, h Harness, s Setup) {
	p := publisher(t, h, s)
	sub := subscriber(t, h, s, 1)
	publish(t, p, s.Destination, "a")

	retried := time.Now()
	require.NoError(t, requireNext(t, sub).Retry(retryDelay))
	d := requireNext(t, sub)
	assert.GreaterOrEqual(t, time.Since(retried), minRetryDelay, "the message came again before its delay")
	assert.Equal(t, "a", string(d.Body()))
	require.NoError(t, d.Ack())
}

func testReject(t *testing.T, h Harness, s Setup) {
	p := publisher(t, h, s)
	sub := subscriber(t, h, s, 1)
	publish(t, p, s.Destination, "a")
	require.NoError(t, requireNext(t, sub).Reject())
	requireNothing(t, sub, "a rejected message must not come again")
}

func testDurable(t *testing.T, h Harness, s Setup) {
	p := publisher(t, h, s)
	first := subscriber(t, h, s, 1)
	publish(t, p, s.Destination, "a")
	require.NoError(t, requireNext(t, first).Ack())
	require.NoError(t, first.Close())

	publish(t, p, s.Destination, "b")
	second := subscriber(t, h, s, 1)
	d := requireNext(t, second)
	assert.Equal(t, "b", string(d.Body()), "the new subscriber starts after the acknowledged message")
	require.NoError(t, d.Ack())
}

func testNextCancel(t *testing.T, h Harness, s Setup) {
	sub := subscriber(t, h, s, 1)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		_, err := sub.Next(ctx)
		done <- err
	}()
	time.Sleep(settle)
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(stop):
		t.Fatal("Next did not return after its context ended")
	}
}

func testInvalidDestinations(t *testing.T, h Harness, s Setup) {
	p := publisher(t, h, s)
	require.NoError(t, p.CheckDestination(s.Destination))
	for _, dest := range h.InvalidDestinations {
		assert.ErrorIs(t, p.CheckDestination(dest), broker.ErrInvalidDestination, "%q", dest)
	}
}

func testTooLarge(t *testing.T, h Harness, s Setup) {
	p := publisher(t, h, s)
	limit := p.MaxMessageSize()
	if limit <= 0 {
		t.Skip("the driver knows no maximum size")
	}
	_, err := p.Publish(t.Context(), broker.Message{Destination: s.Destination, Body: make([]byte, limit+1)})
	require.ErrorIs(t, err, broker.ErrMessageTooLarge, "limit %d", limit)
}
