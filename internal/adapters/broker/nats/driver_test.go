// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package nats_test

import (
	"fmt"
	"log/slog"
	"sync/atomic"
	"testing"

	"github.com/go-playground/validator/v10"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/go-tram/tram/internal/adapters/broker"
	"github.com/go-tram/tram/internal/adapters/broker/brokertest"
	natsdriver "github.com/go-tram/tram/internal/adapters/broker/nats"
	"github.com/go-tram/tram/internal/testutil/natstest"
)

func newDriver() *natsdriver.Driver {
	return natsdriver.New(validator.New(validator.WithRequiredStructEnabled()))
}

func TestConformance(t *testing.T) {
	t.Parallel()
	url := natstest.Start(t)

	nc, err := nats.Connect(url)
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	require.NoError(t, err)

	var n atomic.Int64
	brokertest.Run(t, brokertest.Harness{
		Driver: newDriver(),
		Setup: func(t *testing.T) brokertest.Setup {
			t.Helper()
			name := fmt.Sprintf("conformance%d", n.Add(1))
			// Streams come from outside TRAM, e.g. NACK.
			_, createErr := js.CreateStream(t.Context(),
				jetstream.StreamConfig{Name: name, Subjects: []string{name + ".>"}})
			require.NoError(t, createErr)
			return brokertest.Setup{
				Destination: name + ".logs",
				Publisher:   map[string]any{"url": url},
				Subscriber:  map[string]any{"url": url, "subject": name + ".>", "durable": name},
			}
		},
		InvalidDestinations: []string{"", "a..b", "a.*", "a.>", "a b", "a.", ".a"},
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
		{"publisher with an unknown field", func() error {
			_, err := d.NewPublisher(map[string]any{"url": "nats://127.0.0.1:1", "stream": "s"},
				broker.PublisherConfig{Name: "p", MaxPending: 1}, discard)
			return err
		}},
		{"subscriber without subject", func() error {
			_, err := d.NewSubscriber(map[string]any{"url": "nats://127.0.0.1:1"},
				broker.SubscriberConfig{Name: "s", Capacity: 1}, discard)
			return err
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.ErrorContains(t, tt.run(), "nats options")
		})
	}
}

func TestOpenNeedsTheStream(t *testing.T) {
	t.Parallel()
	url := natstest.Start(t)

	sub, err := newDriver().NewSubscriber(map[string]any{"url": url, "subject": "missing.>"},
		broker.SubscriberConfig{Name: "s", Capacity: 1}, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	t.Cleanup(func() { _ = sub.Close() })
	assert.ErrorContains(t, sub.Open(t.Context()), "failed to find the stream of subject missing.>")
}
