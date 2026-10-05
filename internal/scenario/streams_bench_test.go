package scenario_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"runtime"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"codeberg.org/eljoth/tram/internal/adapters/sink"
	"codeberg.org/eljoth/tram/internal/adapters/source"
	"codeberg.org/eljoth/tram/internal/adapters/stage"
	"codeberg.org/eljoth/tram/internal/core"
	"codeberg.org/eljoth/tram/internal/core/domain"
	"codeberg.org/eljoth/tram/internal/testutil/natstest"
)

const (
	// idleWindow is how long the idle streams are observed. It spans two
	// fetch timeouts of the NATS source (5s), so every worker renews its
	// pull request at least once. It also exceeds the default benchtime, so
	// `go test -bench .` runs each case once.
	idleWindow = 10 * time.Second
	// settleTime lets the streams finish starting before the idle window,
	// and the streams of the previous case finish stopping before the
	// baseline.
	settleTime = 2 * time.Second
	// baselineWindow measures the CPU the process uses without streams, e.g.
	// for the samples themselves, which is subtracted from the idle window.
	baselineWindow = 3 * time.Second
	kibibyte       = 1024
)

// streamShape is the kind of stream that is created many times.
type streamShape struct {
	name string
	// workers of the NATS source; 0 for the OTLP source.
	workers int
	build   func(e *streamsEnv, i int) domain.StreamSpec
}

func streamShapes() []streamShape {
	routing := func(workers int) streamShape {
		return streamShape{
			name:    "nats-filter-nats",
			workers: workers,
			build: func(e *streamsEnv, i int) domain.StreamSpec {
				return domain.StreamSpec{
					Name: fmt.Sprintf("s%d", i),
					Source: domain.SourceSpec{Name: "in", Type: "nats", Spec: map[string]any{
						"url":     e.natsURL,
						"stream":  e.input,
						"subject": fmt.Sprintf("%s.s%d", e.input, i),
						"durable": fmt.Sprintf("%s-s%d", e.input, i),
						"workers": float64(workers),
					}},
					Stages: []domain.StageSpec{{
						Name: "filter", Type: "cel_filter", From: "in",
						Spec: map[string]any{"logs": "record.attributes.exists(a, a.key == 'team')"},
					}},
					Sinks: []domain.SinkSpec{e.sink(i, "filter")},
				}
			},
		}
	}
	return []streamShape{
		routing(1),
		routing(4),
		{
			name: "otlp-batch-nats",
			build: func(e *streamsEnv, i int) domain.StreamSpec {
				return domain.StreamSpec{
					Name: fmt.Sprintf("s%d", i),
					Source: domain.SourceSpec{Name: "in", Type: "otlp", Spec: map[string]any{
						"http_port": float64(e.ports[i]),
					}},
					Stages: []domain.StageSpec{{
						Name: "batch", Type: "batch", From: "in",
						Spec: map[string]any{"max_records": float64(batchMaxRecords), "timeout": batchTimeout},
					}},
					Sinks: []domain.SinkSpec{e.sink(i, "batch")},
				}
			},
		},
	}
}

func streamCounts() []int {
	return []int{10, 100, 1000}
}

// BenchmarkStreams measures what a stream costs while no data flows, to tell
// how many streams one Tram instance can hold before sharding them over
// several instances pays off. Every replica that runs a stream pays this cost
// again, unlike the cost of processing data, which replicas share.
//
// Each case measures the process without streams for baselineWindow, builds
// n streams of one shape in one core, waits for them to start, and observes
// them for idleWindow. It reports per stream, relative to the baseline:
//
//   - build-µs/stream: time to create the stream through the core
//   - heap-KiB/stream: live heap after a garbage collection
//   - goroutines/stream
//   - nats-conns/stream: client connections the NATS server sees
//   - nats-msgs/s/stream: messages the NATS server receives from idle streams,
//     mostly pull requests of NATS source workers
//   - idle-cpu-µs/s/stream: CPU time of the process per second; 1,000,000
//     µs/s are one core
//
// ns/op includes the idle window and has no meaning of its own. Requires
// Docker or Podman; skipped otherwise.
func BenchmarkStreams(b *testing.B) {
	server := natstest.StartServer(b)

	nc, err := nats.Connect(server.URL)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		b.Fatal(err)
	}

	run := 0
	for _, shape := range streamShapes() {
		for _, n := range streamCounts() {
			b.Run(fmt.Sprintf("%s/workers=%d/streams=%d", shape.name, shape.workers, n), func(b *testing.B) {
				for range b.N {
					run++
					e := newStreamsEnv(b, server, js, fmt.Sprintf("streams%d", run))
					measureStreams(b, e, shape, n)
				}
			})
		}
	}
}

// streamsEnv gives each run its own input and output JetStream streams, which
// all Tram streams of the run share.
type streamsEnv struct {
	b             *testing.B
	natsURL       string
	monitoringURL string
	js            jetstream.JetStream
	input, output string
	// ports are free TCP ports for OTLP sources, one per stream.
	ports []int
}

// reservePorts finds n distinct free ports. All listeners stay open until
// every port is found, so the system cannot hand out a port twice, as it can
// with freePort called n times.
func (e *streamsEnv) reservePorts(n int) {
	e.b.Helper()
	listeners := make([]net.Listener, 0, n)
	defer func() {
		for _, l := range listeners {
			_ = l.Close()
		}
	}()
	for range n {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			e.b.Fatal(err)
		}
		listeners = append(listeners, l)
		e.ports = append(e.ports, l.Addr().(*net.TCPAddr).Port)
	}
}

func newStreamsEnv(b *testing.B, server natstest.Server, js jetstream.JetStream, prefix string) *streamsEnv {
	b.Helper()
	e := &streamsEnv{
		b: b, natsURL: server.URL, monitoringURL: server.MonitoringURL, js: js,
		input: prefix + "-in", output: prefix + "-out",
	}
	ctx := context.Background()
	if _, err := js.CreateStream(
		ctx,
		jetstream.StreamConfig{Name: e.input, Subjects: []string{e.input + ".>"}},
	); err != nil {
		b.Fatal(err)
	}
	// Deleting the input stream also removes the consumers of its sources.
	b.Cleanup(func() {
		_ = js.DeleteStream(context.Background(), e.input)
		_ = js.DeleteStream(context.Background(), e.output)
	})
	return e
}

// sink publishes to the shared output stream, on a subject of its own.
func (e *streamsEnv) sink(i int, from string) domain.SinkSpec {
	return domain.SinkSpec{Name: "out", Type: "nats", From: from, Spec: map[string]any{
		"url":    e.natsURL,
		"stream": e.output,
		"subject": map[string]any{
			"register": []any{e.output + ".>"},
			"logs":     fmt.Sprintf("'%s.s%d'", e.output, i),
		},
	}}
}

// sample is the state of the process and the NATS server at one point in time.
type sample struct {
	at         time.Time
	cpu        time.Duration
	heap       uint64
	goroutines int
	conns      int
	natsMsgs   int64
}

func (e *streamsEnv) sample() sample {
	e.b.Helper()
	runtime.GC()
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	v := e.varz()
	return sample{
		at:         time.Now(),
		cpu:        cpuTime(),
		heap:       mem.HeapAlloc,
		goroutines: runtime.NumGoroutine(),
		conns:      v.Connections,
		natsMsgs:   v.InMsgs,
	}
}

// varz holds the fields of the NATS server's /varz endpoint that are used.
type varz struct {
	Connections int   `json:"connections"`
	InMsgs      int64 `json:"in_msgs"`
}

func (e *streamsEnv) varz() varz {
	e.b.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, e.monitoringURL+"/varz", nil)
	if err != nil {
		e.b.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		e.b.Fatal(err)
	}
	defer resp.Body.Close()
	var v varz
	if err = json.NewDecoder(resp.Body).Decode(&v); err != nil {
		e.b.Fatal(err)
	}
	return v
}

func measureStreams(b *testing.B, e *streamsEnv, shape streamShape, n int) {
	b.Helper()
	ctx := context.Background()

	time.Sleep(settleTime)
	baselineStart := e.sample()
	time.Sleep(baselineWindow)
	before := e.sample()
	baselineCPU := cpuRate(baselineStart, before)

	// Reserved just before building, so no other process takes the ports.
	if shape.workers == 0 {
		e.reservePorts(n)
	}
	specs := make([]domain.StreamSpec, n)
	for i := range n {
		specs[i] = shape.build(e, i)
	}

	logger := slog.New(slog.DiscardHandler)
	c := core.New(ctx, logger, source.NewFactory(logger), stage.NewFactory(), sink.NewFactory(logger))
	defer func() { _ = c.Close() }()

	start := time.Now()
	for _, spec := range specs {
		if _, err := c.UpsertStream(ctx, spec); err != nil {
			b.Fatal(err)
		}
	}
	build := time.Since(start)

	if shape.workers > 0 {
		e.waitForConsumers(n)
	}
	time.Sleep(settleTime)

	idleStart := e.sample()
	time.Sleep(idleWindow)
	idleEnd := e.sample()

	perStream := func(v float64) float64 { return v / float64(n) }
	seconds := idleEnd.at.Sub(idleStart.at).Seconds()
	b.ReportMetric(perStream(float64(build.Microseconds())), "build-µs/stream")
	b.ReportMetric(perStream(float64(int64(idleEnd.heap)-int64(before.heap))/kibibyte), "heap-KiB/stream")
	b.ReportMetric(perStream(float64(idleEnd.goroutines-before.goroutines)), "goroutines/stream")
	b.ReportMetric(perStream(float64(idleEnd.conns-before.conns)), "nats-conns/stream")
	b.ReportMetric(perStream(float64(idleEnd.natsMsgs-idleStart.natsMsgs)/seconds), "nats-msgs/s/stream")
	b.ReportMetric(perStream(cpuRate(idleStart, idleEnd)-baselineCPU), "idle-cpu-µs/s/stream")
}

// cpuRate returns the CPU time the process used between two samples, in µs
// per second.
func cpuRate(from, to sample) float64 {
	return float64((to.cpu - from.cpu).Microseconds()) / to.at.Sub(from.at).Seconds()
}

// waitForConsumers waits until every NATS source has created its consumer.
func (e *streamsEnv) waitForConsumers(n int) {
	e.b.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		s, err := e.js.Stream(ctx, e.input)
		if err == nil {
			info, infoErr := s.Info(ctx)
			if infoErr == nil && info.State.Consumers >= n {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	e.b.Fatalf("stream %s did not get %d consumers", e.input, n)
}
