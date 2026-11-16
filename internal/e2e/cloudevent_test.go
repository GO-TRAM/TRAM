// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package e2e_test

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"codeberg.org/eljoth/tram/internal/core/domain"
	"codeberg.org/eljoth/tram/internal/testutil/otlpgen"
)

// The cloudevent sink and source over NATS JetStream. Unlike the nats sink,
// the cloudevent sink creates no streams, so every test sets its stream up
// first, as it would be outside of Tram.
func TestCloudEventNATS(t *testing.T) {
	t.Parallel()

	n := startNATS(t)

	t.Run("sink to source round trip", func(t *testing.T) {
		t.Parallel()

		mid := n.ceStream(t, "mid")
		ingest := newPipeline(t, nil, []domain.SinkSpec{n.ceSinkByService("out", "in", mid)})
		forward := newStream(t, n.ceSourceSpec(mid),
			[]domain.StageSpec{customerFilter("customer-a", "in", isCustomerA)},
			[]domain.SinkSpec{captureSinkSpec("received", "customer-a")})

		require.Equal(t, http.StatusOK,
			ingest.post(t, request("shop", rec{"r1", "A"}, rec{"r2", "B"}, rec{"r3", "A"}), 10*time.Second))
		require.Equal(t, http.StatusOK,
			ingest.post(t, request("cart", rec{"k1", "B"}, rec{"k2", "A"}), 10*time.Second))

		messages := n.read(t, mid)
		require.Len(t, messages, 2, "one CloudEvent per service")
		for _, m := range messages {
			assert.Equal(t, "1.0", m.Header.Get("ce-specversion"))
			assert.Equal(t, "io.opentelemetry.logs", m.Header.Get("ce-type"))
			assert.Equal(t, "tram/out", m.Header.Get("ce-source"))
			assert.Equal(t, m.Subject, m.Header.Get("ce-subject"))
			assert.NotEmpty(t, m.Header.Get("ce-id"))
			assert.Equal(t, "application/x-protobuf", m.Header.Get("content-type"))
		}

		received := forward.sinks.sink(t, "received")
		require.Eventually(t, func() bool { return len(arrived(received.received())) == 3 },
			10*time.Second, 10*time.Millisecond)
		assert.ElementsMatch(t, []string{"shop/e2e/r1", "shop/e2e/r3", "cart/e2e/k2"}, arrived(received.received()))
	})

	t.Run("signal types survive the round trip", func(t *testing.T) {
		t.Parallel()

		mid := n.ceStream(t, "signals")
		ingest := newPipeline(t, nil, []domain.SinkSpec{{
			Name: "out", Type: "cloudevent", From: "in", Spec: n.ceSinkSpec(map[string]any{
				"logs":    fmt.Sprintf("'%s.logs'", mid),
				"metrics": fmt.Sprintf("'%s.metrics'", mid),
				"traces":  fmt.Sprintf("'%s.traces'", mid),
			}),
		}})
		forward := newStream(t, n.ceSourceSpec(mid), nil, []domain.SinkSpec{captureSinkSpec("received", "in")})

		shape := otlpgen.Shape{Records: 2, Resources: 1, Teams: 1}
		require.Equal(t, http.StatusOK, ingest.postTo(t, "/v1/logs", otlpgen.Logs(shape), 10*time.Second))
		require.Equal(t, http.StatusOK, ingest.postTo(t, "/v1/metrics", otlpgen.Metrics(shape), 10*time.Second))
		require.Equal(t, http.StatusOK, ingest.postTo(t, "/v1/traces", otlpgen.Traces(shape), 10*time.Second))

		var types []string
		for _, m := range n.read(t, mid) {
			types = append(types, m.Subject+"="+m.Header.Get("ce-type"))
		}
		assert.ElementsMatch(t, []string{
			mid + ".logs=io.opentelemetry.logs",
			mid + ".metrics=io.opentelemetry.metrics",
			mid + ".traces=io.opentelemetry.traces",
		}, types)

		received := forward.sinks.sink(t, "received")
		require.Eventually(t, func() bool { return len(received.received()) == 3 }, 10*time.Second, 10*time.Millisecond)
		var signals []string
		for _, e := range received.received() {
			switch {
			case e.Logs != nil && e.Metrics == nil && e.Traces == nil:
				signals = append(signals, "logs")
			case e.Metrics != nil && e.Logs == nil && e.Traces == nil:
				signals = append(signals, "metrics")
			case e.Traces != nil && e.Logs == nil && e.Metrics == nil:
				signals = append(signals, "traces")
			default:
				t.Errorf("event with an unexpected mix of signal types: %+v", e)
			}
			assert.Equal(t, 2, e.RecordCount())
		}
		assert.ElementsMatch(t, []string{"logs", "metrics", "traces"}, signals)
	})

	t.Run("rejected events are not redelivered", func(t *testing.T) {
		t.Parallel()

		mid := n.ceStream(t, "rejected")
		ingest := newPipeline(t, nil, []domain.SinkSpec{n.ceSinkByService("out", "in", mid)})
		forward := newStream(t, n.ceSourceSpec(mid), nil, []domain.SinkSpec{{
			Name: "refuse", Type: captureType, From: "in", Spec: map[string]any{"fail": "permanent"},
		}})
		require.Equal(t, http.StatusOK, ingest.post(t, request("shop", rec{"r1", "A"}), 10*time.Second))

		consumer, err := n.js.Consumer(t.Context(), mid, mid)
		require.Eventually(t, func() bool {
			if err != nil {
				consumer, err = n.js.Consumer(t.Context(), mid, mid)
				return false
			}
			info, infoErr := consumer.Info(t.Context())
			return infoErr == nil && info.AckFloor.Stream == 1 && info.NumAckPending == 0 && info.NumRedelivered == 0
		}, 10*time.Second, 50*time.Millisecond)
		assert.Empty(t, forward.sinks.sink(t, "refuse").received())
	})

	t.Run("a destination without a stream reaches the client", func(t *testing.T) {
		t.Parallel()

		p := newPipeline(t, nil, []domain.SinkSpec{{
			Name: "out", Type: "cloudevent", From: "in",
			Spec: n.ceSinkSpec(map[string]any{"logs": "'nowhere.logs'"}),
		}})
		assert.Equal(t, http.StatusServiceUnavailable, p.post(t, request("shop", rec{"r1", "A"}), 10*time.Second))
	})

	t.Run("an invalid destination rejects the event", func(t *testing.T) {
		t.Parallel()

		mid := n.ceStream(t, "invalid")
		p := newPipeline(t, nil, []domain.SinkSpec{{
			Name: "out", Type: "cloudevent", From: "in",
			Spec: n.ceSinkSpec(map[string]any{"logs": fmt.Sprintf("'%s.' + 'a b'", mid)}),
		}})
		assert.Equal(t, http.StatusUnprocessableEntity, p.post(t, request("shop", rec{"r1", "A"}), 10*time.Second))
		assert.Empty(t, n.read(t, mid))
	})

	t.Run("stopping an idle source is fast", func(t *testing.T) {
		t.Parallel()

		mid := n.ceStream(t, "idle")
		forward := newStream(t, n.ceSourceSpec(mid), nil, []domain.SinkSpec{captureSinkSpec("received", "in")})
		time.Sleep(500 * time.Millisecond)

		start := time.Now()
		require.NoError(t, forward.core.DeleteStream(context.Background(), "e2e"))
		assert.Less(t, time.Since(start), time.Second)
	})
}

// ceStream creates a stream for name's subjects, as done outside of Tram.
func (n *natsServer) ceStream(t *testing.T, name string) string {
	t.Helper()
	stream := n.stream(name)
	_, err := n.js.CreateStream(t.Context(), jetstream.StreamConfig{Name: stream, Subjects: []string{stream + ".>"}})
	require.NoError(t, err)
	return stream
}

func (n *natsServer) ceSinkSpec(destination map[string]any) map[string]any {
	return map[string]any{
		"broker":      "nats",
		"nats":        map[string]any{"url": n.url},
		"destination": destination,
	}
}

func (n *natsServer) ceSinkByService(name, from, stream string) domain.SinkSpec {
	return domain.SinkSpec{Name: name, Type: "cloudevent", From: from, Spec: n.ceSinkSpec(map[string]any{
		"logs": fmt.Sprintf("'%s.' + logs.resource_logs[0].resource.attributes.get_attr('service.name')", stream),
	})}
}

func (n *natsServer) ceSourceSpec(stream string) domain.SourceSpec {
	return domain.SourceSpec{Name: "in", Type: "cloudevent", Spec: map[string]any{
		"broker":  "nats",
		"workers": float64(4),
		"nats":    map[string]any{"url": n.url, "subject": stream + ".>", "durable": stream},
	}}
}
