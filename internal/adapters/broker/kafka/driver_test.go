// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package kafka_test

import (
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/go-playground/validator/v10"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"codeberg.org/eljoth/tram/internal/adapters/broker"
	"codeberg.org/eljoth/tram/internal/adapters/broker/brokertest"
	kafkadriver "codeberg.org/eljoth/tram/internal/adapters/broker/kafka"
	"codeberg.org/eljoth/tram/internal/testutil/kafkatest"
)

func newDriver() *kafkadriver.Driver {
	return kafkadriver.New(validator.New(validator.WithRequiredStructEnabled()))
}

func TestConformance(t *testing.T) {
	t.Parallel()
	brokers := kafkatest.Start(t)
	seeds := make([]any, len(brokers))
	for i, b := range brokers {
		seeds[i] = b
	}

	var n atomic.Int64
	brokertest.Run(t, brokertest.Harness{
		Driver: newDriver(),
		Setup: func(t *testing.T) brokertest.Setup {
			t.Helper()
			topic := fmt.Sprintf("conformance-%d", n.Add(1))
			// Topics come from outside TRAM.
			kafkatest.CreateTopic(t, brokers, topic, 1)
			return brokertest.Setup{
				Destination: topic,
				Publisher:   map[string]any{"brokers": seeds, "max_message_bytes": float64(64 << 10)},
				Subscriber:  map[string]any{"brokers": seeds, "topic": topic, "group": topic},
			}
		},
		InvalidDestinations: []string{"", ".", "..", "a b", "a/b", "a*", strings.Repeat("a", 250)},
	})
}

func TestOptionsAreValidated(t *testing.T) {
	t.Parallel()

	d := newDriver()
	discard := slog.New(slog.DiscardHandler)
	brokers := []any{"127.0.0.1:1"}
	tests := []struct {
		name string
		run  func() error
	}{
		{"publisher without brokers", func() error {
			_, err := d.NewPublisher(map[string]any{}, broker.PublisherConfig{Name: "p", MaxPending: 1}, discard)
			return err
		}},
		{"publisher with an empty broker", func() error {
			_, err := d.NewPublisher(map[string]any{"brokers": []any{""}},
				broker.PublisherConfig{Name: "p", MaxPending: 1}, discard)
			return err
		}},
		{"publisher with an unknown field", func() error {
			_, err := d.NewPublisher(map[string]any{"brokers": brokers, "topic": "t"},
				broker.PublisherConfig{Name: "p", MaxPending: 1}, discard)
			return err
		}},
		{"max_message_bytes above 1 GiB", func() error {
			_, err := d.NewPublisher(map[string]any{"brokers": brokers, "max_message_bytes": float64(1 << 31)},
				broker.PublisherConfig{Name: "p", MaxPending: 1}, discard)
			return err
		}},
		{"unknown SASL mechanism", func() error {
			_, err := d.NewPublisher(map[string]any{"brokers": brokers, "sasl": map[string]any{
				"mechanism": "gssapi", "username": "u", "password": "p",
			}}, broker.PublisherConfig{Name: "p", MaxPending: 1}, discard)
			return err
		}},
		{"subscriber without group", func() error {
			_, err := d.NewSubscriber(map[string]any{"brokers": brokers, "topic": "t"},
				broker.SubscriberConfig{Name: "s", Capacity: 1}, discard)
			return err
		}},
		{"subscriber without topic", func() error {
			_, err := d.NewSubscriber(map[string]any{"brokers": brokers, "group": "g"},
				broker.SubscriberConfig{Name: "s", Capacity: 1}, discard)
			return err
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.ErrorContains(t, tt.run(), "kafka options")
		})
	}
}

func TestOptionsWithTLSAndSASLAreAccepted(t *testing.T) {
	t.Parallel()

	for _, mechanism := range []string{"plain", "scram-sha-256", "scram-sha-512"} {
		p, err := newDriver().NewPublisher(map[string]any{
			"brokers": []any{"127.0.0.1:1"},
			"tls":     map[string]any{},
			"sasl":    map[string]any{"mechanism": mechanism, "username": "u", "password": "p"},
		}, broker.PublisherConfig{Name: "p", MaxPending: 1}, slog.New(slog.DiscardHandler))
		require.NoError(t, err, mechanism)
		_ = p.Close()
	}
}

func TestOpenNeedsTheTopic(t *testing.T) {
	t.Parallel()
	brokers := kafkatest.Start(t)

	sub, err := newDriver().NewSubscriber(
		map[string]any{"brokers": []any{brokers[0]}, "topic": "missing", "group": "g"},
		broker.SubscriberConfig{Name: "s", Capacity: 1}, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	t.Cleanup(func() { _ = sub.Close() })
	assert.ErrorContains(t, sub.Open(t.Context()), "topic does not exist: missing")
}

// Kafka commits positions, not messages: a record finished before an earlier
// one must not commit past that earlier one, or a restart would lose it.
func TestOutOfOrderAcksLoseNothing(t *testing.T) {
	t.Parallel()
	brokers := kafkatest.Start(t)
	kafkatest.CreateTopic(t, brokers, "ordered", 1)
	discard := slog.New(slog.DiscardHandler)
	d := newDriver()

	p, err := d.NewPublisher(map[string]any{"brokers": []any{brokers[0]}},
		broker.PublisherConfig{Name: "p", MaxPending: 4}, discard)
	require.NoError(t, err)
	t.Cleanup(func() { _ = p.Close() })
	for _, body := range []string{"first", "second", "third"} {
		ack, publishErr := p.Publish(t.Context(), broker.Message{Destination: "ordered", Body: []byte(body)})
		require.NoError(t, publishErr)
		require.NoError(t, ack.Wait(t.Context()))
	}

	opts := map[string]any{"brokers": []any{brokers[0]}, "topic": "ordered", "group": "ordered"}
	first, err := d.NewSubscriber(opts, broker.SubscriberConfig{Name: "s", Capacity: 3}, discard)
	require.NoError(t, err)
	require.NoError(t, first.Open(t.Context()))
	got := map[string]broker.Delivery{}
	for range 3 {
		delivery, nextErr := first.Next(t.Context())
		require.NoError(t, nextErr)
		got[string(delivery.Body())] = delivery
	}
	// The second and third finish; the first is still being handled when
	// the subscriber stops.
	require.NoError(t, got["third"].Ack())
	require.NoError(t, got["second"].Reject())
	require.NoError(t, first.Close())

	second, err := d.NewSubscriber(opts, broker.SubscriberConfig{Name: "s", Capacity: 3}, discard)
	require.NoError(t, err)
	t.Cleanup(func() { _ = second.Close() })
	require.NoError(t, second.Open(t.Context()))
	var again []string
	for range 3 {
		delivery, nextErr := second.Next(t.Context())
		require.NoError(t, nextErr)
		again = append(again, string(delivery.Body()))
		require.NoError(t, delivery.Ack())
	}
	assert.Equal(t, []string{"first", "second", "third"}, again,
		"nothing was committed past the open first record, so all come again")
}
