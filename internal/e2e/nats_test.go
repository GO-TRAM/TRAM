// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package e2e_test

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"codeberg.org/eljoth/tram/internal/core/domain"
	"codeberg.org/eljoth/tram/internal/testutil/natstest"
	"codeberg.org/eljoth/tram/internal/testutil/otlpgen"
)

const signalHeader = "Tram-Signal"

func TestNATS(t *testing.T) {
	t.Parallel()

	n := startNATS(t)

	t.Run("batch then publish one message per service", func(t *testing.T) {
		t.Parallel()

		out := n.stream("out")
		p := newPipeline(t,
			[]domain.StageSpec{batchStage(6, "5s")},
			[]domain.SinkSpec{n.sinkByService("out", "batch", out)})

		codes := p.postConcurrently(t,
			request("checkout", rec{"c1", "A"}, rec{"c2", "B"}),
			request("cart", rec{"k1", "A"}, rec{"k2", "B"}),
			request("search", rec{"s1", "A"}, rec{"s2", "B"}),
		)
		assert.Equal(t, []int{200, 200, 200}, codes, "requests are answered once the batch is in JetStream")

		messages := n.read(t, out)
		require.Len(t, messages, 3, "one message per service")
		assert.Equal(t, map[string][]string{
			out + ".checkout": {"checkout/e2e/c1", "checkout/e2e/c2"},
			out + ".cart":     {"cart/e2e/k1", "cart/e2e/k2"},
			out + ".search":   {"search/e2e/s1", "search/e2e/s2"},
		}, bySubject(t, messages))
	})

	t.Run("split into two streams", func(t *testing.T) {
		t.Parallel()

		outA, outRest := n.stream("a"), n.stream("rest")
		p := newPipeline(t,
			[]domain.StageSpec{
				customerFilter("customer-a", "in", isCustomerA),
				customerFilter("other-customers", "in", "!"+isCustomerA),
			},
			[]domain.SinkSpec{
				n.sinkByService("sink-a", "customer-a", outA),
				n.sinkByService("sink-rest", "other-customers", outRest),
			})

		require.Equal(
			t,
			http.StatusOK,
			p.post(t, request("shop", rec{"r1", "A"}, rec{"r2", "B"}, rec{"r3", "A"}, rec{"r4", "C"}), 10*time.Second),
		)

		assert.Equal(
			t,
			map[string][]string{outA + ".shop": {"shop/e2e/r1", "shop/e2e/r3"}},
			bySubject(t, n.read(t, outA)),
		)
		assert.Equal(
			t,
			map[string][]string{outRest + ".shop": {"shop/e2e/r2", "shop/e2e/r4"}},
			bySubject(t, n.read(t, outRest)),
		)
	})

	t.Run("sinks share a stream set up beforehand", func(t *testing.T) {
		t.Parallel()

		shared := n.stream("shared")
		_, err := n.js.CreateStream(t.Context(), jetstream.StreamConfig{
			Name:     shared,
			Subjects: []string{shared + ".other"},
			MaxAge:   time.Hour,
		})
		require.NoError(t, err)

		sink := func(name, branch string) domain.SinkSpec {
			prefix := shared + "." + branch
			return domain.SinkSpec{Name: name, Type: "nats", From: "in", Spec: map[string]any{
				"url":    n.url,
				"stream": shared,
				"subject": map[string]any{
					"register": []any{prefix + ".>"},
					"logs": fmt.Sprintf(
						"'%s.' + logs.resource_logs[0].resource.attributes.get_attr('service.name')",
						prefix,
					),
				},
			}}
		}
		p := newPipeline(t, nil, []domain.SinkSpec{sink("sink-a", "a"), sink("sink-b", "b")})

		require.Equal(t, http.StatusOK, p.post(t, request("shop", rec{"r1", "A"}), 10*time.Second))
		assert.Equal(t, map[string][]string{
			shared + ".a.shop": {"shop/e2e/r1"},
			shared + ".b.shop": {"shop/e2e/r1"},
		}, bySubject(t, n.read(t, shared)))

		stream, err := n.js.Stream(t.Context(), shared)
		require.NoError(t, err)
		config := stream.CachedInfo().Config
		assert.ElementsMatch(t, []string{shared + ".other", shared + ".a.>", shared + ".b.>"}, config.Subjects)
		assert.Equal(t, time.Hour, config.MaxAge, "limits set beforehand are kept")
	})

	t.Run("sink creates a compressed stream with limits", func(t *testing.T) {
		t.Parallel()

		out := n.stream("compressed")
		spec := n.sinkSpec(out, map[string]any{
			"logs": fmt.Sprintf("'%s.' + logs.resource_logs[0].resource.attributes.get_attr('service.name')", out),
		})
		// An interest stream drops messages nobody consumes yet, so they could
		// not be read back here.
		spec["retention"] = "limits"
		spec["max_age"] = "4h"
		spec["size"] = float64(1 << 30)
		spec["compression"] = "s2"
		p := newPipeline(t, nil, []domain.SinkSpec{{Name: "out", Type: "nats", From: "in", Spec: spec}})

		require.Equal(t, http.StatusOK, p.post(t, request("shop", rec{"r1", "A"}, rec{"r2", "B"}), 10*time.Second))
		assert.Equal(
			t,
			map[string][]string{out + ".shop": {"shop/e2e/r1", "shop/e2e/r2"}},
			bySubject(t, n.read(t, out)),
		)

		stream, err := n.js.Stream(t.Context(), out)
		require.NoError(t, err)
		config := stream.CachedInfo().Config
		assert.Equal(t, jetstream.S2Compression, config.Compression)
		assert.Equal(t, jetstream.LimitsPolicy, config.Retention)
		assert.Equal(t, 4*time.Hour, config.MaxAge)
		assert.Equal(t, int64(1<<30), config.MaxBytes)
	})

	t.Run("sink to source round trip", func(t *testing.T) {
		t.Parallel()

		mid := n.stream("mid")
		ingest := newPipeline(t, nil, []domain.SinkSpec{n.sinkByService("out", "in", mid)})
		forward := newStream(t, n.sourceSpec(mid), []domain.StageSpec{customerFilter("customer-a", "in", isCustomerA)},
			[]domain.SinkSpec{captureSinkSpec("received", "customer-a")})

		require.Equal(
			t,
			http.StatusOK,
			ingest.post(t, request("shop", rec{"r1", "A"}, rec{"r2", "B"}, rec{"r3", "A"}), 10*time.Second),
		)
		require.Equal(t, http.StatusOK, ingest.post(t, request("cart", rec{"k1", "B"}, rec{"k2", "A"}), 10*time.Second))

		received := forward.sinks.sink(t, "received")
		require.Eventually(
			t,
			func() bool { return len(arrived(received.received())) == 3 },
			10*time.Second,
			10*time.Millisecond,
		)
		assert.ElementsMatch(t, []string{"shop/e2e/r1", "shop/e2e/r3", "cart/e2e/k2"}, arrived(received.received()))
	})

	t.Run("routing by record attribute", func(t *testing.T) {
		t.Parallel()
		testRoutingByRecordAttribute(t, n)
	})

	t.Run("signal types survive the round trip", func(t *testing.T) {
		t.Parallel()

		mid := n.stream("signals")
		ingest := newPipeline(t, nil, []domain.SinkSpec{{
			Name: "out", Type: "nats", From: "in", Spec: n.sinkSpec(mid, map[string]any{
				"logs":    fmt.Sprintf("'%s.logs'", mid),
				"metrics": fmt.Sprintf("'%s.metrics'", mid),
				"traces":  fmt.Sprintf("'%s.traces'", mid),
			}),
		}})
		forward := newStream(t, n.sourceSpec(mid), nil, []domain.SinkSpec{captureSinkSpec("received", "in")})

		shape := otlpgen.Shape{Records: 2, Resources: 1, Teams: 1}
		require.Equal(t, http.StatusOK, ingest.postTo(t, "/v1/logs", otlpgen.Logs(shape), 10*time.Second))
		require.Equal(t, http.StatusOK, ingest.postTo(t, "/v1/metrics", otlpgen.Metrics(shape), 10*time.Second))
		require.Equal(t, http.StatusOK, ingest.postTo(t, "/v1/traces", otlpgen.Traces(shape), 10*time.Second))

		var headers []string
		for _, m := range n.read(t, mid) {
			headers = append(headers, m.Subject+"="+m.Header.Get(signalHeader))
		}
		assert.ElementsMatch(t, []string{mid + ".logs=logs", mid + ".metrics=metrics", mid + ".traces=traces"}, headers)

		received := forward.sinks.sink(t, "received")
		require.Eventually(t, func() bool { return len(received.received()) == 3 }, 10*time.Second, 10*time.Millisecond)
		var signals []string
		for _, e := range received.received() {
			switch {
			case e.Logs != nil && e.Metrics == nil && e.Traces == nil:
				signals = append(signals, "logs")
				assert.Equal(
					t,
					"log message 0",
					e.Logs.GetResourceLogs()[0].GetScopeLogs()[0].GetLogRecords()[0].GetBody().GetStringValue(),
				)
			case e.Metrics != nil && e.Logs == nil && e.Traces == nil:
				signals = append(signals, "metrics")
				assert.Equal(
					t,
					"metric_0",
					e.Metrics.GetResourceMetrics()[0].GetScopeMetrics()[0].GetMetrics()[0].GetName(),
				)
			case e.Traces != nil && e.Logs == nil && e.Metrics == nil:
				signals = append(signals, "traces")
				assert.Equal(t, "span-0", e.Traces.GetResourceSpans()[0].GetScopeSpans()[0].GetSpans()[0].GetName())
			default:
				t.Errorf("event with an unexpected mix of signal types: %+v", e)
			}
			assert.Equal(t, 2, e.RecordCount())
		}
		assert.ElementsMatch(t, []string{"logs", "metrics", "traces"}, signals)
	})

	t.Run("workers fill a batch", func(t *testing.T) {
		t.Parallel()

		const messages = 300
		mid := n.stream("workers")
		_, err := n.js.CreateStream(t.Context(), jetstream.StreamConfig{Name: mid, Subjects: []string{mid + ".>"}})
		require.NoError(t, err)
		_, payload, err := (&domain.Event{Logs: otlpgen.Logs(otlpgen.Shape{Records: 1, Resources: 1})}).MarshalOTLPData()
		require.NoError(t, err)
		for range messages {
			_, err = n.js.PublishMsg(t.Context(), &nats.Msg{
				Subject: mid + ".logs",
				Data:    payload,
				Header:  nats.Header{signalHeader: []string{string(domain.SignalLogs)}},
			})
			require.NoError(t, err)
		}

		in := n.sourceSpec(mid)
		in.Spec["workers"] = float64(messages)
		forward := newStream(t, in, []domain.StageSpec{batchStage(messages, "30s")},
			[]domain.SinkSpec{captureSinkSpec("received", "batch")})

		received := forward.sinks.sink(t, "received")
		require.Eventually(t, func() bool { return len(received.received()) > 0 }, 10*time.Second, 10*time.Millisecond,
			"the batch was not filled by the messages in flight")
		events := received.received()
		require.Len(t, events, 1)
		assert.Equal(t, messages, events[0].RecordCount())

		cons, err := n.js.Consumer(t.Context(), mid, mid)
		require.NoError(t, err)
		require.Eventually(t, func() bool {
			info, infoErr := cons.Info(t.Context())
			return infoErr == nil && info.NumAckPending == 0 && info.NumPending == 0
		}, 10*time.Second, 10*time.Millisecond, "the messages were not acknowledged")
	})

	t.Run("at most workers messages are buffered", func(t *testing.T) {
		t.Parallel()

		const messages, workers = 200, 20
		mid := n.stream("waiting")
		_, err := n.js.CreateStream(t.Context(), jetstream.StreamConfig{Name: mid, Subjects: []string{mid + ".>"}})
		require.NoError(t, err)
		_, payload, err := (&domain.Event{Logs: otlpgen.Logs(otlpgen.Shape{Records: 1, Resources: 1})}).MarshalOTLPData()
		require.NoError(t, err)
		for range messages {
			_, err = n.js.PublishMsg(t.Context(), &nats.Msg{
				Subject: mid + ".logs",
				Data:    payload,
				Header:  nats.Header{signalHeader: []string{string(domain.SignalLogs)}},
			})
			require.NoError(t, err)
		}

		in := n.sourceSpec(mid)
		in.Spec["workers"] = float64(workers)
		held := captureSinkSpec("received", "in")
		held.Spec = map[string]any{"hold": true}
		forward := newStream(t, in, nil, []domain.SinkSpec{held})
		received := forward.sinks.sink(t, "received")

		t.Cleanup(received.release)

		var cons jetstream.Consumer
		require.Eventually(t, func() bool {
			cons, err = n.js.Consumer(t.Context(), mid, mid)
			return err == nil
		}, 10*time.Second, 5*time.Millisecond, "the source did not create its consumer")

		var infoMu sync.Mutex
		counts := func() (int, uint64) {
			infoMu.Lock()
			defer infoMu.Unlock()
			info, infoErr := cons.Info(t.Context())
			if infoErr != nil {
				return -1, 0
			}
			return info.NumAckPending, info.NumPending
		}
		require.Eventually(t, func() bool {
			out, _ := counts()
			return out >= workers
		}, 10*time.Second, 5*time.Millisecond, "the workers did not all receive a message")
		// Besides the messages being handled, the source buffers at most one
		// more per worker.
		assert.Never(t, func() bool {
			out, _ := counts()
			return out > 2*workers
		}, 500*time.Millisecond, 10*time.Millisecond, "more messages were out than workers and buffer hold")
		_, waiting := counts()
		assert.GreaterOrEqual(t, waiting, uint64(messages-2*workers), "the other messages wait in the stream")

		received.release()
		require.Eventually(t, func() bool {
			out, left := counts()
			return out == 0 && left == 0
		}, 20*time.Second, 5*time.Millisecond, "the messages were not acknowledged")
	})

	t.Run("stopping an idle source is fast", func(t *testing.T) {
		t.Parallel()

		mid := n.stream("idle")
		_ = newPipeline(t, nil, []domain.SinkSpec{n.sinkByService("out", "in", mid)})
		forward := newStream(t, n.sourceSpec(mid), nil, []domain.SinkSpec{captureSinkSpec("received", "in")})
		time.Sleep(500 * time.Millisecond)

		start := time.Now()
		require.NoError(t, forward.core.DeleteStream(context.Background(), "e2e"))
		assert.Less(t, time.Since(start), time.Second)
	})

	t.Run("publish failure reaches the client", func(t *testing.T) {
		t.Parallel()

		out := n.stream("narrow")
		p := newPipeline(t, nil, []domain.SinkSpec{{
			Name: "out", Type: "nats", From: "in", Spec: map[string]any{
				"url":    n.url,
				"stream": out,
				"subject": map[string]any{
					"register": []any{out + ".allowed"},
					"logs":     fmt.Sprintf("'%s.elsewhere'", out),
				},
			},
		}})

		assert.Equal(t, http.StatusServiceUnavailable, p.post(t, request("shop", rec{"r1", "A"}), 10*time.Second))
		assert.Empty(t, n.read(t, out))
	})
}

type natsServer struct {
	url     string
	js      jetstream.JetStream
	streams atomic.Int64
}

func startNATS(t *testing.T) *natsServer {
	t.Helper()
	url := natstest.Start(t)
	nc, err := nats.Connect(url)
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	require.NoError(t, err)
	return &natsServer{url: url, js: js}
}

func (n *natsServer) stream(name string) string {
	return fmt.Sprintf("e2e%d-%s", n.streams.Add(1), name)
}

func (n *natsServer) sinkSpec(stream string, subjects map[string]any) map[string]any {
	subjects["register"] = []any{stream + ".>"}
	return map[string]any{"url": n.url, "stream": stream, "subject": subjects}
}

func (n *natsServer) sinkByService(name, from, stream string) domain.SinkSpec {
	return domain.SinkSpec{Name: name, Type: "nats", From: from, Spec: n.sinkSpec(stream, map[string]any{
		"logs": fmt.Sprintf("'%s.' + logs.resource_logs[0].resource.attributes.get_attr('service.name')", stream),
	})}
}

func (n *natsServer) sourceSpec(stream string) domain.SourceSpec {
	return domain.SourceSpec{Name: "in", Type: "nats", Spec: map[string]any{
		"url":     n.url,
		"stream":  stream,
		"subject": stream + ".>",
		"durable": stream,
	}}
}

func (n *natsServer) read(t *testing.T, name string) []*jetstream.RawStreamMsg {
	t.Helper()
	ctx := context.Background()
	stream, err := n.js.Stream(ctx, name)
	if err != nil {
		return nil
	}
	info, err := stream.Info(ctx)
	require.NoError(t, err)

	var messages []*jetstream.RawStreamMsg
	for seq := info.State.FirstSeq; seq <= info.State.LastSeq && info.State.Msgs > 0; seq++ {
		m, getErr := stream.GetMsg(ctx, seq)
		require.NoError(t, getErr)
		messages = append(messages, m)
	}
	return messages
}

func bySubject(t *testing.T, messages []*jetstream.RawStreamMsg) map[string][]string {
	t.Helper()
	out := map[string][]string{}
	for _, m := range messages {
		event, err := domain.UnmarshalOTLPData(domain.Signal(m.Header.Get(signalHeader)), m.Data)
		require.NoError(t, err)
		require.NotNil(t, event.Logs, "message on %s is not logs", m.Subject)
		out[m.Subject] = append(out[m.Subject], arrived([]*domain.Event{event})...)
	}
	for subject := range out {
		sort.Strings(out[subject])
	}
	return out
}
