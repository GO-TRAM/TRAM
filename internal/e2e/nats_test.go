package e2e_test

import (
	"context"
	"fmt"
	"net/http"
	"sort"
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

// signalHeader must match the header of Tram's NATS sink and source.
const signalHeader = "Tram-Signal"

// TestNATS runs pipelines against a NATS server in a container and checks what
// actually ends up in JetStream. All subtests share the container and use
// their own streams. Requires Docker or Podman; skipped otherwise.
func TestNATS(t *testing.T) {
	t.Parallel()

	n := startNATS(t)

	t.Run("batch then publish one message per service", func(t *testing.T) {
		t.Parallel()

		out := n.stream("out")
		p := newPipeline(t,
			[]domain.StageSpec{batchStage(6, "1h")},
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

	// Two sinks share a JetStream stream that was set up beforehand. Each adds
	// its subjects, and neither removes the other's or resets the stream's
	// limits.
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

	// What Tram's NATS sink publishes is consumed by Tram's NATS source in a
	// second stream, filtered there, and arrives with resource and scope.
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

	// Logs, metrics and traces keep their type through NATS. Decoding them
	// without the signal header used to turn everything into logs.
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

		// The JetStream messages carry the signal type.
		var headers []string
		for _, m := range n.read(t, mid) {
			headers = append(headers, m.Subject+"="+m.Header.Get(signalHeader))
		}
		assert.ElementsMatch(t, []string{mid + ".logs=logs", mid + ".metrics=metrics", mid + ".traces=traces"}, headers)

		// The source decodes each message as its own signal type.
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

	// Stopping waits for messages being processed, not for a pending fetch.
	// It used to take the full fetch timeout of 5 seconds, holding up every
	// other change to the core.
	t.Run("stopping an idle source is fast", func(t *testing.T) {
		t.Parallel()

		mid := n.stream("idle")
		_ = newPipeline(t, nil, []domain.SinkSpec{n.sinkByService("out", "in", mid)}) // creates the JetStream stream
		forward := newStream(t, n.sourceSpec(mid), nil, []domain.SinkSpec{captureSinkSpec("received", "in")})
		time.Sleep(500 * time.Millisecond) // the source is now waiting in a fetch

		start := time.Now()
		require.NoError(t, forward.core.DeleteStream(context.Background(), "e2e"))
		assert.Less(t, time.Since(start), time.Second)
	})

	// A subject outside the stream's register cannot be published; the client
	// is told the data was not delivered.
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

// natsServer is a NATS server in a container with a JetStream client.
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

// stream returns a stream name not used by any other subtest.
func (n *natsServer) stream(name string) string {
	return fmt.Sprintf("e2e%d-%s", n.streams.Add(1), name)
}

func (n *natsServer) sinkSpec(stream string, subjects map[string]any) map[string]any {
	subjects["register"] = []any{stream + ".>"}
	return map[string]any{"url": n.url, "stream": stream, "subject": subjects}
}

// sinkByService publishes logs to "<stream>.<service.name>".
func (n *natsServer) sinkByService(name, from, stream string) domain.SinkSpec {
	return domain.SinkSpec{Name: name, Type: "nats", From: from, Spec: n.sinkSpec(stream, map[string]any{
		"logs": fmt.Sprintf("'%s.' + logs.resource_logs[0].resource.attributes.get_attr('service.name')", stream),
	})}
}

// sourceSpec consumes everything published to the stream.
func (n *natsServer) sourceSpec(stream string) domain.SourceSpec {
	return domain.SourceSpec{Name: "in", Type: "nats", Spec: map[string]any{
		"url":     n.url,
		"stream":  stream,
		"subject": stream + ".>",
		"durable": stream,
	}}
}

// read returns every message in the stream, in order. A stream that was never
// created counts as empty.
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

// bySubject decodes log messages the way Tram's NATS source does and lists
// their records per subject.
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
