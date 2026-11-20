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

	"github.com/go-tram/tram/internal/core/domain"
	"github.com/go-tram/tram/internal/testutil/kafkatest"
)

// The broker sink and source with the Kafka driver. TRAM creates no topics,
// so every test sets its topics up first.
func TestBrokerKafka(t *testing.T) {
	t.Parallel()

	k := &kafkaServer{brokers: kafkatest.Start(t)}

	t.Run("sink to source round trip", func(t *testing.T) {
		t.Parallel()

		topic := k.topic(t, "logs")
		ingest := newPipeline(t, nil, []domain.SinkSpec{{Name: "out", Type: "broker", From: "in",
			Spec: k.sinkSpec(map[string]any{"logs": fmt.Sprintf("'%s'", topic)})}})
		forward := newStream(t, k.source(topic, "logs"),
			[]domain.StageSpec{customerFilter("customer-a", "in", isCustomerA)},
			[]domain.SinkSpec{captureSinkSpec("received", "customer-a")})

		require.Equal(t, http.StatusOK,
			ingest.post(t, request("shop", rec{"r1", "A"}, rec{"r2", "B"}, rec{"r3", "A"}), 10*time.Second))
		require.Equal(t, http.StatusOK, ingest.post(t, request("cart", rec{"k1", "B"}, rec{"k2", "A"}), 10*time.Second))

		received := forward.sinks.sink(t, "received")
		require.Eventually(t, func() bool { return len(arrived(received.received())) == 3 },
			30*time.Second, 50*time.Millisecond)
		assert.ElementsMatch(t, []string{"shop/e2e/r1", "shop/e2e/r3", "cart/e2e/k2"}, arrived(received.received()))
	})

	t.Run("routing to a topic per service", func(t *testing.T) {
		t.Parallel()

		prefix := fmt.Sprintf("e2e%d-", k.topics.Add(1))
		shop, cart := prefix+"shop", prefix+"cart"
		kafkatest.CreateTopic(t, k.brokers, shop, 1)
		kafkatest.CreateTopic(t, k.brokers, cart, 1)
		ingest := newPipeline(t, nil, []domain.SinkSpec{{Name: "out", Type: "broker", From: "in",
			Spec: k.sinkSpec(map[string]any{
				"logs": fmt.Sprintf(
					"'%s' + logs.resource_logs[0].resource.attributes.get_attr('service.name')",
					prefix,
				),
			})}})
		received := map[string]*captureSink{}
		for _, topic := range []string{shop, cart} {
			s := newStream(t, k.source(topic, "logs"), nil, []domain.SinkSpec{captureSinkSpec("received", "in")})
			received[topic] = s.sinks.sink(t, "received")
		}

		require.Equal(t, []int{200, 200}, ingest.postConcurrently(t,
			request("shop", rec{"s1", "A"}), request("cart", rec{"c1", "B"})))
		for topic, want := range map[string]string{shop: "shop/e2e/s1", cart: "cart/e2e/c1"} {
			require.Eventually(t, func() bool { return len(arrived(received[topic].received())) == 1 },
				30*time.Second, 50*time.Millisecond, topic)
			assert.Equal(t, []string{want}, arrived(received[topic].received()))
		}
	})

	t.Run("an invalid topic rejects the event", func(t *testing.T) {
		t.Parallel()

		p := newPipeline(t, nil, []domain.SinkSpec{{Name: "out", Type: "broker", From: "in",
			Spec: k.sinkSpec(map[string]any{"logs": "'not a topic'"})}})
		assert.Equal(t, http.StatusUnprocessableEntity, p.post(t, request("shop", rec{"r1", "A"}), 10*time.Second))
	})
}

type kafkaServer struct {
	brokers []string
	topics  atomic.Int64
}

// topic creates a topic, as done outside TRAM.
func (k *kafkaServer) topic(t *testing.T, name string) string {
	t.Helper()
	topic := fmt.Sprintf("e2e%d-%s", k.topics.Add(1), name)
	kafkatest.CreateTopic(t, k.brokers, topic, 1)
	return topic
}

func (k *kafkaServer) seeds() []any {
	seeds := make([]any, len(k.brokers))
	for i, b := range k.brokers {
		seeds[i] = b
	}
	return seeds
}

func (k *kafkaServer) sinkSpec(destination map[string]any) map[string]any {
	return map[string]any{
		"driver":      "kafka",
		"kafka":       map[string]any{"brokers": k.seeds()},
		"destination": destination,
	}
}

func (k *kafkaServer) source(topic, signal string) domain.SourceSpec {
	return domain.SourceSpec{Name: "in", Type: "broker", Spec: map[string]any{
		"driver": "kafka",
		"signal": signal,
		"kafka":  map[string]any{"brokers": k.seeds(), "topic": topic, "group": topic},
	}}
}
