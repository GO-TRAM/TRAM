// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package amqp_test

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"codeberg.org/eljoth/tram/internal/adapters/broker"
	amqpdriver "codeberg.org/eljoth/tram/internal/adapters/broker/amqp"
	"codeberg.org/eljoth/tram/internal/adapters/broker/brokertest"
	"codeberg.org/eljoth/tram/internal/testutil/amqptest"
	"codeberg.org/eljoth/tram/internal/testutil/tcpproxy"
)

const unreachableURL = "amqp://127.0.0.1:1"

func newDriver() *amqpdriver.Driver {
	return amqpdriver.New(validator.New(validator.WithRequiredStructEnabled()))
}

func TestConformance(t *testing.T) {
	t.Parallel()
	b := amqptest.Start(t)

	var n atomic.Int64
	brokertest.Run(t, brokertest.Harness{
		Driver: newDriver(),
		Setup: func(t *testing.T) brokertest.Setup {
			t.Helper()
			// Queues come from outside TRAM.
			address := fmt.Sprintf("conformance.%d", n.Add(1))
			b.CreateQueue(t, address)
			return brokertest.Setup{
				Destination: address,
				Publisher:   map[string]any{"url": b.URL, "max_message_bytes": float64(64 << 10)},
				Subscriber:  map[string]any{"url": b.URL, "address": address},
			}
		},
		InvalidDestinations: []string{"", "a b", "a\tb", "a\nb"},
	})
}

func TestOptionsAreValidated(t *testing.T) {
	t.Parallel()

	d := newDriver()
	discard := slog.New(slog.DiscardHandler)
	tests := []struct {
		name string
		run  func() error
	}{
		{"publisher without url", func() error {
			_, err := d.NewPublisher(map[string]any{}, broker.PublisherConfig{Name: "p", MaxPending: 1}, discard)
			return err
		}},
		{"publisher with a username but no password", func() error {
			_, err := d.NewPublisher(map[string]any{"url": unreachableURL, "username": "u"},
				broker.PublisherConfig{Name: "p", MaxPending: 1}, discard)
			return err
		}},
		{"publisher with an unknown field", func() error {
			_, err := d.NewPublisher(map[string]any{"url": unreachableURL, "address": "a"},
				broker.PublisherConfig{Name: "p", MaxPending: 1}, discard)
			return err
		}},
		{"subscriber without address", func() error {
			_, err := d.NewSubscriber(map[string]any{"url": unreachableURL},
				broker.SubscriberConfig{Name: "s", Capacity: 1}, discard)
			return err
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.ErrorContains(t, tt.run(), "amqp options")
		})
	}
}

// go-amqp does not reconnect by itself; the driver does. A publisher and a
// subscriber work again after the broker was unreachable.
func TestReconnectsAfterAnOutage(t *testing.T) {
	t.Parallel()
	b := amqptest.Start(t)
	b.CreateQueue(t, "outage")
	p := tcpproxy.New(t, strings.TrimPrefix(b.URL, "amqp://"))
	url := "amqp://" + p.Addr()
	discard := slog.New(slog.DiscardHandler)
	d := newDriver()

	pub, err := d.NewPublisher(map[string]any{"url": url}, broker.PublisherConfig{Name: "p", MaxPending: 4}, discard)
	require.NoError(t, err)
	t.Cleanup(func() { _ = pub.Close() })
	sub, err := d.NewSubscriber(map[string]any{"url": url, "address": "outage"},
		broker.SubscriberConfig{Name: "s", Capacity: 1}, discard)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sub.Close() })
	require.NoError(t, sub.Open(t.Context()))

	publish := func(body string) error {
		ack, publishErr := pub.Publish(t.Context(), broker.Message{Destination: "outage", Body: []byte(body)})
		if publishErr != nil {
			return publishErr
		}
		return ack.Wait(t.Context())
	}
	receive := func() string {
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
		defer cancel()
		delivery, nextErr := sub.Next(ctx)
		require.NoError(t, nextErr)
		require.NoError(t, delivery.Ack())
		return string(delivery.Body())
	}

	require.NoError(t, publish("before"))
	assert.Equal(t, "before", receive())

	p.Cut()
	require.Eventually(t, func() bool { return publish("lost") != nil }, 10*time.Second, 50*time.Millisecond,
		"publishing fails while the broker is unreachable")

	p.Restore()
	require.Eventually(t, func() bool { return publish("after") == nil }, 20*time.Second, 100*time.Millisecond,
		"the publisher connects again")
	assert.Equal(t, "after", receive(), "the subscriber connects again")
}
