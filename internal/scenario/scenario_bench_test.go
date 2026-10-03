package scenario_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"slices"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"

	"codeberg.org/eljoth/tram/internal/adapters/sink"
	"codeberg.org/eljoth/tram/internal/adapters/source"
	"codeberg.org/eljoth/tram/internal/adapters/stage"
	"codeberg.org/eljoth/tram/internal/core"
	"codeberg.org/eljoth/tram/internal/core/domain"
	"codeberg.org/eljoth/tram/internal/testutil/natstest"
	"codeberg.org/eljoth/tram/internal/testutil/otlpgen"
)

// signalHeader must match the header of Tram's NATS sink and source.
const signalHeader = "Tram-Signal"

// scenario is a pipeline benchmarked end to end.
type scenario struct {
	name string
	// source is "otlp" (requests over HTTP) or "nats" (messages in JetStream).
	source string
	// teams is the number of distinct "team" attribute values in the payload.
	teams int
	// stages and sinks hang off the source named "in". Sinks get their output
	// stream from the env.
	build func(e *env) ([]domain.StageSpec, []domain.SinkSpec)
}

func scenarios() []scenario {
	return []scenario{
		{
			name:   "otlp-nats",
			source: "otlp",
			teams:  1,
			build: func(e *env) ([]domain.StageSpec, []domain.SinkSpec) {
				return nil, []domain.SinkSpec{e.natsSink("out", "in")}
			},
		},
		{
			name:   "otlp-batch-nats",
			source: "otlp",
			teams:  1,
			build: func(e *env) ([]domain.StageSpec, []domain.SinkSpec) {
				return []domain.StageSpec{{
					Name: "batch", Type: "batch", From: "in",
					Spec: map[string]any{"max_records": float64(batchMaxRecords), "timeout": batchTimeout},
				}}, []domain.SinkSpec{e.natsSink("out", "batch")}
			},
		},
		{
			name:   "otlp-split-nats",
			source: "otlp",
			teams:  2,
			build: func(e *env) ([]domain.StageSpec, []domain.SinkSpec) {
				const teamZero = "record.attributes.exists(a, a.key == 'team' && a.value.string_value == 'team-0')"
				return []domain.StageSpec{
					{Name: "team-zero", Type: "cel_filter", From: "in", Spec: map[string]any{"logs": teamZero}},
					{Name: "other-teams", Type: "cel_filter", From: "in", Spec: map[string]any{"logs": "!" + teamZero}},
				}, []domain.SinkSpec{
					e.natsSink("out-a", "team-zero"),
					e.natsSink("out-b", "other-teams"),
				}
			},
		},
		{
			name:   "nats-filter-nats",
			source: "nats",
			teams:  1,
			build: func(e *env) ([]domain.StageSpec, []domain.SinkSpec) {
				return []domain.StageSpec{{
					Name: "filter", Type: "cel_filter", From: "in",
					Spec: map[string]any{"logs": "record.attributes.exists(a, a.key == 'team')"},
				}}, []domain.SinkSpec{e.natsSink("out", "filter")}
			},
		},
	}
}

// The batch stage is configured to flush 1,000 records or after 10ms, so
// single callers do not wait for the 200ms default timeout.
const (
	batchMaxRecords = 1000
	batchTimeout    = "10ms"
)

func concurrencies() []int {
	return []int{1, 4, 8}
}

func shapes() []otlpgen.Shape {
	return []otlpgen.Shape{
		{Records: 1, Resources: 1, Attributes: 4},
		{Records: 100, Resources: 1, Attributes: 4},
		{Records: 1000, Resources: 10, Attributes: 4},
	}
}

// BenchmarkScenario runs every scenario for every concurrency and payload
// size. One iteration is one OTLP request or one NATS message. Besides
// throughput, it reports the process's CPU time per record, which includes
// the HTTP client generating the load, and request latencies.
//
// Requires Docker or Podman; skipped otherwise.
func BenchmarkScenario(b *testing.B) {
	natsURL := natstest.Start(b)

	nc, err := nats.Connect(natsURL)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		b.Fatal(err)
	}

	var runs atomic.Int64
	for _, sc := range scenarios() {
		for _, concurrency := range concurrencies() {
			for _, shape := range shapes() {
				shape.Teams = sc.teams
				name := fmt.Sprintf("%s/concurrency=%d/%s", sc.name, concurrency, shape)
				b.Run(name, func(b *testing.B) {
					e := newEnv(b, natsURL, js, &runs)
					switch sc.source {
					case "otlp":
						benchmarkOTLP(b, e, sc, shape, concurrency)
					case "nats":
						benchmarkNATS(b, e, sc, shape, concurrency)
					}
				})
			}
		}
	}
}

// env gives each benchmark run its own NATS streams, so runs do not see each
// other's messages.
type env struct {
	b       *testing.B
	natsURL string
	js      jetstream.JetStream
	prefix  string
	streams []string
}

// newEnv numbers the environments with runs, so their stream names are unique.
func newEnv(b *testing.B, natsURL string, js jetstream.JetStream, runs *atomic.Int64) *env {
	e := &env{b: b, natsURL: natsURL, js: js, prefix: fmt.Sprintf("bench%d", runs.Add(1))}
	b.Cleanup(func() {
		for _, s := range e.streams {
			_ = js.DeleteStream(context.Background(), s)
		}
	})
	return e
}

func (e *env) stream(suffix string) string {
	name := e.prefix + "-" + suffix
	e.streams = append(e.streams, name)
	return name
}

// natsSink publishes to its own stream, one subject per service.
func (e *env) natsSink(name, from string) domain.SinkSpec {
	stream := e.stream(name)
	return domain.SinkSpec{Name: name, Type: "nats", From: from, Spec: map[string]any{
		"url":       e.natsURL,
		"stream":    stream,
		"size":      float64(256 << 20),
		"retention": "1m",
		"subject": map[string]any{
			"register": []any{stream + ".>"},
			"logs": fmt.Sprintf(
				"'%s.' + logs.resource_logs[0].resource.attributes.get_attr('service.name')",
				stream,
			),
		},
	}}
}

// startStream builds the stream through the core, like `tram server` does.
func startStream(b *testing.B, in domain.SourceSpec, stages []domain.StageSpec, sinks []domain.SinkSpec) {
	logger := slog.New(slog.DiscardHandler)
	c := core.New(context.Background(), logger, source.NewFactory(logger), stage.NewFactory(), sink.NewFactory(logger))
	b.Cleanup(func() { _ = c.Close() })
	_, err := c.UpsertStream(
		context.Background(),
		domain.StreamSpec{Name: "scenario", Source: in, Stages: stages, Sinks: sinks},
	)
	if err != nil {
		b.Fatal(err)
	}
}

func benchmarkOTLP(b *testing.B, e *env, sc scenario, shape otlpgen.Shape, concurrency int) {
	port := freePort(b)
	stages, sinks := sc.build(e)
	startStream(
		b,
		domain.SourceSpec{Name: "in", Type: "otlp", Spec: map[string]any{"http_port": float64(port)}},
		stages,
		sinks,
	)

	body, err := proto.Marshal(otlpgen.Logs(shape))
	if err != nil {
		b.Fatal(err)
	}
	url := fmt.Sprintf("http://127.0.0.1:%d/v1/logs", port)
	client := &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: concurrency}}
	b.Cleanup(client.CloseIdleConnections)

	send := func() (time.Duration, error) {
		start := time.Now()
		resp, postErr := client.Post(url, "application/x-protobuf", bytes.NewReader(body))
		if postErr != nil {
			return 0, postErr
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return 0, fmt.Errorf("status %s", resp.Status)
		}
		return time.Since(start), nil
	}
	if _, sendErr := send(); sendErr != nil { // warm-up, also waits for the server
		b.Fatal(sendErr)
	}

	latencies := make([]time.Duration, b.N)
	var next atomic.Int64
	var wg sync.WaitGroup

	b.ReportAllocs()
	b.ResetTimer()
	cpuStart := cpuTime()
	for range concurrency {
		wg.Go(func() {
			for {
				i := next.Add(1) - 1
				if i >= int64(b.N) {
					return
				}
				latency, sendErr := send()
				if sendErr != nil {
					b.Error(sendErr)
					return
				}
				latencies[i] = latency
			}
		})
	}
	wg.Wait()
	cpu := cpuTime() - cpuStart
	b.StopTimer()

	report(b, shape, cpu)
	slices.Sort(latencies)
	b.ReportMetric(float64(percentile(latencies, 0.50).Microseconds())/1000, "p50-ms")
	b.ReportMetric(float64(percentile(latencies, 0.99).Microseconds())/1000, "p99-ms")
}

func benchmarkNATS(b *testing.B, e *env, sc scenario, shape otlpgen.Shape, concurrency int) {
	ctx := context.Background()
	input := e.stream("in")
	if _, err := e.js.CreateStream(
		ctx,
		jetstream.StreamConfig{Name: input, Subjects: []string{input + ".logs"}},
	); err != nil {
		b.Fatal(err)
	}
	signal, payload, err := (&domain.Event{Logs: otlpgen.Logs(shape)}).MarshalOTLPData()
	if err != nil {
		b.Fatal(err)
	}
	publish(b, e.js, input+".logs", signal, payload, b.N)

	stages, sinks := sc.build(e)
	output := e.streams[len(e.streams)-1]
	// Every message has one subject per service, so the sink publishes one
	// message per resource.
	want := uint64(b.N * shape.Resources)

	b.ReportAllocs()
	b.ResetTimer()
	cpuStart := cpuTime()
	// Building the stream is timed too, because it starts consuming at once.
	// It takes milliseconds, against about a second of benchmark time.
	startStream(b, domain.SourceSpec{Name: "in", Type: "nats", Spec: map[string]any{
		"url":     e.natsURL,
		"stream":  input,
		"subject": input + ".logs",
		"durable": input,
		"workers": float64(concurrency),
	}}, stages, sinks)
	waitForMessages(b, e.js, output, want)
	cpu := cpuTime() - cpuStart
	b.StopTimer()

	report(b, shape, cpu)
}

// report adds throughput and CPU time per record. records/s/core is how many
// records one fully used CPU core can process.
func report(b *testing.B, shape otlpgen.Shape, cpu time.Duration) {
	records := float64(shape.Records * b.N)
	b.ReportMetric(records/b.Elapsed().Seconds(), "records/s")
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "events/s")
	b.ReportMetric(float64(cpu.Nanoseconds())/records, "cpu-ns/record")
	b.ReportMetric(records/cpu.Seconds(), "records/s/core")
}

// cpuTime returns the user and system CPU time the process has used.
func cpuTime() time.Duration {
	var usage syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &usage)
	return time.Duration(usage.Utime.Nano() + usage.Stime.Nano())
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	return sorted[min(len(sorted)-1, int(float64(len(sorted))*p))]
}

func freePort(b *testing.B) int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// publish stores n copies of payload in the stream, the way Tram's NATS sink
// writes them.
func publish(b *testing.B, js jetstream.JetStream, subject string, signal domain.Signal, payload []byte, n int) {
	b.Helper()
	acks := make([]jetstream.PubAckFuture, 0, n)
	for range n {
		ack, err := js.PublishMsgAsync(&nats.Msg{
			Subject: subject,
			Data:    payload,
			Header:  nats.Header{signalHeader: []string{string(signal)}},
		})
		if err != nil {
			b.Fatal(err)
		}
		acks = append(acks, ack)
	}
	for _, ack := range acks {
		select {
		case <-ack.Ok():
		case err := <-ack.Err():
			b.Fatal(err)
		}
	}
}

// waitForMessages polls the stream until it holds want messages.
func waitForMessages(b *testing.B, js jetstream.JetStream, stream string, want uint64) {
	b.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		s, err := js.Stream(ctx, stream)
		if err == nil {
			info, infoErr := s.Info(ctx)
			if infoErr == nil && info.State.Msgs >= want {
				return
			}
		}
		time.Sleep(2 * time.Millisecond)
	}
	b.Fatalf("stream %s did not receive %d messages", stream, want)
}
