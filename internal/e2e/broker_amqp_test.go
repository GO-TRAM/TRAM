// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package e2e_test

import (
	"fmt"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"codeberg.org/tram/tram/internal/core/domain"
	"codeberg.org/tram/tram/internal/testutil/amqptest"
)

// The broker sink and source with the AMQP 1.0 driver against ActiveMQ
// Artemis. TRAM creates no queues, so every test sets its queues up first.
func TestBrokerAMQP(t *testing.T) {
	t.Parallel()

	a := &amqpServer{broker: amqptest.Start(t)}

	t.Run("sink to source round trip", func(t *testing.T) {
		t.Parallel()

		queue := a.queue(t, "logs")
		ingest := newPipeline(t, nil, []domain.SinkSpec{{Name: "out", Type: "broker", From: "in",
			Spec: a.sinkSpec(map[string]any{"logs": fmt.Sprintf("'%s'", queue)})}})
		forward := newStream(t, a.source(queue, "logs"),
			[]domain.StageSpec{customerFilter("customer-a", "in", isCustomerA)},
			[]domain.SinkSpec{captureSinkSpec("received", "customer-a")})

		require.Equal(t, http.StatusOK,
			ingest.post(t, request("shop", rec{"r1", "A"}, rec{"r2", "B"}, rec{"r3", "A"}), 10*time.Second))
		require.Equal(t, http.StatusOK, ingest.post(t, request("cart", rec{"k1", "B"}, rec{"k2", "A"}), 10*time.Second))

		received := forward.sinks.sink(t, "received")
		require.Eventually(t, func() bool { return len(arrived(received.received())) == 3 },
			20*time.Second, 50*time.Millisecond)
		assert.ElementsMatch(t, []string{"shop/e2e/r1", "shop/e2e/r3", "cart/e2e/k2"}, arrived(received.received()))
	})

	t.Run("routing to a queue per service", func(t *testing.T) {
		t.Parallel()

		prefix := fmt.Sprintf("e2e%d.", a.queues.Add(1))
		shop, cart := prefix+"shop", prefix+"cart"
		a.broker.CreateQueue(t, shop)
		a.broker.CreateQueue(t, cart)
		ingest := newPipeline(t, nil, []domain.SinkSpec{{Name: "out", Type: "broker", From: "in",
			Spec: a.sinkSpec(map[string]any{
				"logs": fmt.Sprintf(
					"'%s' + logs.resource_logs[0].resource.attributes.get_attr('service.name')", prefix),
			})}})
		received := map[string]*captureSink{}
		for _, queue := range []string{shop, cart} {
			s := newStream(t, a.source(queue, "logs"), nil, []domain.SinkSpec{captureSinkSpec("received", "in")})
			received[queue] = s.sinks.sink(t, "received")
		}

		require.Equal(t, []int{200, 200}, ingest.postConcurrently(t,
			request("shop", rec{"s1", "A"}), request("cart", rec{"c1", "B"})))
		for queue, want := range map[string]string{shop: "shop/e2e/s1", cart: "cart/e2e/c1"} {
			require.Eventually(t, func() bool { return len(arrived(received[queue].received())) == 1 },
				20*time.Second, 50*time.Millisecond, queue)
			assert.Equal(t, []string{want}, arrived(received[queue].received()))
		}
	})

	t.Run("an invalid address rejects the event", func(t *testing.T) {
		t.Parallel()

		p := newPipeline(t, nil, []domain.SinkSpec{{Name: "out", Type: "broker", From: "in",
			Spec: a.sinkSpec(map[string]any{"logs": "'not an address'"})}})
		assert.Equal(t, http.StatusUnprocessableEntity, p.post(t, request("shop", rec{"r1", "A"}), 10*time.Second))
	})
}

type amqpServer struct {
	broker *amqptest.Broker
	queues atomic.Int64
}

// queue creates a queue, as done outside TRAM.
func (a *amqpServer) queue(t *testing.T, name string) string {
	t.Helper()
	queue := fmt.Sprintf("e2e%d.%s", a.queues.Add(1), name)
	a.broker.CreateQueue(t, queue)
	return queue
}

func (a *amqpServer) sinkSpec(destination map[string]any) map[string]any {
	return map[string]any{
		"driver":      "amqp",
		"amqp":        map[string]any{"url": a.broker.URL},
		"destination": destination,
	}
}

func (a *amqpServer) source(queue, signal string) domain.SourceSpec {
	return domain.SourceSpec{Name: "in", Type: "broker", Spec: map[string]any{
		"driver": "amqp",
		"signal": signal,
		"amqp":   map[string]any{"url": a.broker.URL, "address": queue},
	}}
}
