// Command loadgen generates OTLP load shaped like production traffic for the
// tlmr load test in setup/tlmr-loadtest.
//
//	loadgen configs -ids 500 -routers 50 -out setup/tlmr-loadtest/config
//	loadgen send -url http://localhost:4318 -rps 100
//	loadgen receive -listen :4319
//
//nolint:mnd,goconst // Flag defaults and stream spec literals read best as written.
package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha1" //nolint:gosec // Derives stable test IDs, not a security use.
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

const (
	statsInterval = 10 * time.Second
	filePerm      = 0o644
	dirPerm       = 0o755
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	if len(os.Args) < 2 {
		return errors.New("usage: loadgen configs|send|receive [flags]")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	switch os.Args[1] {
	case "configs":
		return configs(os.Args[2:])
	case "send":
		return send(ctx, os.Args[2:])
	case "receive":
		return receive(ctx, os.Args[2:])
	default:
		return fmt.Errorf("unknown mode %q", os.Args[1])
	}
}

// resourceID returns the i-th resource ID. configs and send derive the same
// IDs, so every ID the sender uses has a link.
func resourceID(i int) string {
	h := sha1.Sum([]byte(fmt.Sprintf("tlmr-resource-%d", i))) //nolint:gosec // See import.
	return fmt.Sprintf("%x-%x-%x-%x-%x", h[0:4], h[4:6], h[6:8], h[8:10], h[10:16])
}

func routerID(i int) string {
	h := sha1.Sum([]byte(fmt.Sprintf("tlmr-router-%d", i))) //nolint:gosec // See import.
	return fmt.Sprintf("%x-%x-%x-%x-%x", h[0:4], h[4:6], h[6:8], h[8:10], h[10:16])
}

// --- configs ----------------------------------------------------------------

func configs(args []string) error {
	fs := flag.NewFlagSet("configs", flag.ExitOnError)
	ids := fs.Int("ids", 500, "number of resource IDs, one link each")
	routers := fs.Int("routers", 50, "number of routers; resource IDs are spread round-robin")
	out := fs.String("out", "config", "directory to write the stream files to")
	destinations := fs.String("destinations", "http://openobserve-destinations:5080/api/default",
		"OTLP endpoint the routers deliver to")
	openobserve := fs.String("openobserve", "http://openobserve:5080/api/default",
		"OTLP endpoint for audit logs and metrics")
	auth := fs.String(
		"auth",
		"Basic cm9vdEBleGFtcGxlLmNvbTpDb21wbGV4cGFzcyMxMjM=",
		"Authorization header for OpenObserve",
	)
	size := fs.Int64("size", 15<<30, "size limit of each NATS stream in bytes")
	compression := fs.String("compression", "none", "compression of the NATS streams: none or s2")
	maxAge := fs.String("max-age", "5m", "maximum age of messages in the NATS streams")
	_ = fs.Parse(args)

	if err := os.MkdirAll(*out, dirPerm); err != nil {
		return err
	}
	old, err := filepath.Glob(filepath.Join(*out, "*.json"))
	if err != nil {
		return err
	}
	for _, f := range old {
		if err = os.Remove(f); err != nil {
			return err
		}
	}

	files := map[string]any{
		"otlp_to_nats":                otlpToNATS(*size, *maxAge, *compression),
		"nats_logs_to_openobserve":    auditToOpenObserve(*openobserve, *auth),
		"nats_metrics_to_openobserve": metricsToOpenObserve(*openobserve, *auth),
	}
	for i := range *ids {
		id := resourceID(i)
		files["link-"+id] = link(id, routerID(i%*routers), *size, *maxAge, *compression)
	}
	for r := range *routers {
		id := routerID(r)
		files["router-"+id] = router(id, *destinations, *auth)
	}
	for name, stream := range files {
		data, marshalErr := json.MarshalIndent(map[string]any{"stream": stream}, "", "  ")
		if marshalErr != nil {
			return marshalErr
		}
		if err = os.WriteFile(filepath.Join(*out, name+".json"), append(data, '\n'), filePerm); err != nil {
			return err
		}
	}
	log.Printf("wrote %d stream files to %s", len(files), *out)
	return nil
}

type obj = map[string]any

// stdout writes the stats as JSON lines.
var stdout = json.NewEncoder(os.Stdout) //nolint:gochecknoglobals // Shared by both modes' stats.

func natsSource(name, stream, subject, durable string, workers int) obj {
	return obj{"name": name, "type": "nats", "spec": obj{
		"url": "nats://nats:4222", "stream": stream, "subject": subject, "durable": durable, "workers": workers,
	}}
}

func batch(name, from string, maxRecords int, timeout string) obj {
	return obj{"name": name, "type": "batch", "from": from, "spec": obj{"max_records": maxRecords, "timeout": timeout}}
}

func hasID(attrs, id string) string {
	return fmt.Sprintf("%s.exists(a, a.key == 'stackit.resource.id' && a.value.string_value == '%s')", attrs, id)
}

// dataPoints is the attributes of a metric's data point. The generator sends
// gauges with one data point each, where the resource ID is a data point
// attribute.
const dataPoints = "metric.gauge.data_points[0].attributes"

func hasNonEmpty(attrs, key string) string {
	return fmt.Sprintf("%s.exists(a, a.key == '%s' && a.value.string_value != '')", attrs, key)
}

func otlpToNATS(size int64, maxAge, compression string) obj {
	return obj{
		"source": obj{"name": "otlp", "type": "otlp", "spec": obj{"http_port": 4318}},
		"stages": []obj{
			{"name": "validate-attributes", "type": "cel_validator", "from": "otlp", "spec": obj{
				"logs": "logs.resource_logs.all(r, r.scope_logs.all(s, s.log_records.all(record, " +
					hasNonEmpty("record.attributes", "stackit.log.type") + " && " +
					hasNonEmpty("record.attributes", "stackit.resource.id") + ")))",
				"traces": "traces.resource_spans.all(r, r.scope_spans.all(s, s.spans.all(span, " +
					hasNonEmpty("span.attributes", "stackit.visibility") + " && " +
					hasNonEmpty("span.attributes", "stackit.resource.id") + ")))",
				"metrics": "metrics.resource_metrics.all(r, r.scope_metrics.all(s, s.metrics.all(m, " +
					"m.gauge.data_points.size() > 0 && m.gauge.data_points.all(dp, " +
					hasNonEmpty("dp.attributes", "stackit.visibility") + " && " +
					hasNonEmpty("dp.attributes", "stackit.resource.id") + "))))",
				"on_mismatch": "reject",
			}},
			batch("batch", "validate-attributes", 1000, "200ms"),
		},
		"sinks": []obj{{"name": "nats", "type": "nats", "from": "batch", "spec": obj{
			"url": "nats://nats:4222", "stream": "telemetry",
			"subject": obj{
				"register": []string{"logs.*.*", "traces.*.*", "metrics.*.*"},
				"logs": "'logs.' + record.attributes.get_attr('stackit.log.type') + '.' + " +
					"record.attributes.get_attr('stackit.resource.id')",
				"traces": "'traces.' + span.attributes.get_attr('stackit.visibility') + '.' + " +
					"span.attributes.get_attr('stackit.resource.id')",
				"metrics": "'metrics.' + " + dataPoints + ".get_attr('stackit.visibility') + '.' + " +
					dataPoints + ".get_attr('stackit.resource.id')",
			},
			"retention": "limits", "max_age": maxAge, "size": size, "compression": compression,
		}}},
	}
}

func link(id, router string, size int64, maxAge, compression string) obj {
	return obj{
		"source": natsSource(id+"-source", "telemetry", "*.*."+id, id, 10),
		"stages": []obj{
			{"name": id + "-validate", "type": "cel_validator", "from": id + "-source", "spec": obj{
				"logs": "logs.resource_logs.all(r, r.scope_logs.all(s, s.log_records.all(record, " + hasID(
					"record.attributes",
					id,
				) + ")))",
				"traces": "traces.resource_spans.all(r, r.scope_spans.all(s, s.spans.all(span, " + hasID(
					"span.attributes",
					id,
				) + ")))",
				"metrics": "metrics.resource_metrics.all(r, r.scope_metrics.all(s, s.metrics.all(m, " +
					"m.gauge.data_points.all(dp, " + hasID("dp.attributes", id) + "))))",
				"on_mismatch": "reject",
			}},
			batch(id+"-batch", id+"-validate", 500, "200ms"),
		},
		"sinks": []obj{{"name": "router-" + router, "type": "nats", "from": id + "-batch", "spec": obj{
			"url": "nats://nats:4222", "stream": "router",
			"subject": obj{
				"register": []string{"router.*." + router},
				"logs":     "'router.logs." + router + "'",
				"traces":   "'router.traces." + router + "'",
				"metrics":  "'router.metrics." + router + "'",
			},
			"retention": "limits", "max_age": maxAge, "size": size, "compression": compression,
		}}},
	}
}

// router delivers to the shared destinations instance. Tenants are told apart
// by stackit.resource.id, which every record carries.
func router(id, destinations, auth string) obj {
	return obj{
		"source": natsSource(id+"-source", "router", "router.*."+id, id, 200),
		"stages": []obj{batch(id+"-batch", id+"-source", 5000, "1s")},
		"sinks": []obj{{"name": id + "-out", "type": "otlp", "from": id + "-batch", "spec": obj{
			"url": destinations, "headers": obj{"Authorization": auth, "stream-name": "tram"},
		}}},
	}
}

func auditToOpenObserve(url, auth string) obj {
	return obj{
		"source": natsSource("nats-audit", "telemetry", "logs.AUDIT.*", "nats_logs_to_openobserve", 200),
		"stages": []obj{batch("batch-audit", "nats-audit", 5000, "1s")},
		"sinks": []obj{{"name": "openobserve-audit", "type": "otlp", "from": "batch-audit", "spec": obj{
			"url": url, "headers": obj{"Authorization": auth, "stream-name": "tram"},
		}}},
	}
}

func metricsToOpenObserve(url, auth string) obj {
	return obj{
		"source": natsSource("nats-metrics", "telemetry", "metrics.*.*", "nats_metrics_to_openobserve", 200),
		"stages": []obj{batch("batch-metrics", "nats-metrics", 5000, "1s")},
		"sinks": []obj{{"name": "openobserve-metrics", "type": "otlp", "from": "batch-metrics", "spec": obj{
			"url": url, "headers": obj{"Authorization": auth},
		}}},
	}
}

// --- send -------------------------------------------------------------------

// template is a request built up front. Its timestamps are set when it is
// sent, so receivers see current data however long the generator runs.
type template struct {
	path    string
	msg     proto.Message
	records int
}

// request is a template encoded for sending.
type request struct {
	path      string
	body      []byte
	records   int
	rawBytes  int
	gzipBytes int
}

func send(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("send", flag.ExitOnError)
	url := fs.String("url", "http://localhost:4318", "OTLP/HTTP endpoint")
	rps := fs.Float64("rps", 100, "requests per second")
	concurrency := fs.Int("concurrency", 200, "maximum requests in flight")
	records := fs.Int("records", 5, "records per request")
	ids := fs.Int("ids", 500, "resource IDs to choose from")
	idsPerRequest := fs.Int("ids-per-request", 3, "distinct resource IDs per request")
	bodyBytes := fs.Int("body-bytes", 20000, "uncompressed size of a log body or span attribute value")
	traceShare := fs.Float64("traces", 0.3, "share of requests that carry traces")
	metricShare := fs.Float64("metrics", 0, "share of requests that carry metrics")
	auditShare := fs.Float64("audit", 0.2, "share of log records of type AUDIT")
	pool := fs.Int("pool", 2000, "distinct requests built up front and sent in rotation")
	duration := fs.Duration("duration", 0, "how long to send; 0 sends until interrupted")
	seed := fs.Uint64("seed", 1, "random seed")
	gzipLevel := fs.Int("gzip-level", gzip.DefaultCompression,
		"gzip level from 1 (fastest) to 9 (smallest); -1 is the default level of OpenTelemetry exporters")
	_ = fs.Parse(args)
	if *gzipLevel < gzip.DefaultCompression || *gzipLevel > gzip.BestCompression || *gzipLevel == 0 {
		return fmt.Errorf("gzip-level %d is not between 1 and 9 or -1", *gzipLevel)
	}

	sh := shares{traces: *traceShare, metrics: *metricShare, audit: *auditShare}
	templates, err := buildTemplates(*seed, *pool, *bodyBytes, *records, *ids, *idsPerRequest, sh, *gzipLevel)
	if err != nil {
		return err
	}

	if *duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *duration)
		defer cancel()
	}

	client := &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{MaxIdleConnsPerHost: *concurrency}}
	var st sendStats
	slots := make(chan struct{}, *concurrency)
	var wg sync.WaitGroup
	go st.report(ctx)

	ticker := time.NewTicker(time.Duration(float64(time.Second) / *rps))
	defer ticker.Stop()
	for i := 0; ; i++ {
		select {
		case <-ctx.Done():
			wg.Wait()
			st.print("final")
			return nil
		case <-ticker.C:
		}
		select {
		case slots <- struct{}{}:
		default:
			st.skipped.Add(1)
			continue
		}
		t := templates[i%len(templates)]
		wg.Go(func() {
			defer func() { <-slots }()
			r, encodeErr := encode(t, *gzipLevel)
			if encodeErr != nil {
				log.Printf("encode: %v", encodeErr)
				return
			}
			st.do(ctx, client, *url, r)
		})
	}
}

// buildTemplates builds the requests the sender rotates through and logs their
// average size.
func buildTemplates(
	seed uint64,
	pool, bodyBytes, records, ids, idsPerRequest int,
	sh shares,
	gzipLevel int,
) ([]template, error) {
	rng := rand.New(rand.NewPCG(seed, seed)) //nolint:gosec // Load shape, not security.
	texts := corpus(rng, 64, bodyBytes)
	templates := make([]template, pool)
	var raw, gz int
	for i := range templates {
		templates[i] = build(rng, texts, records, ids, idsPerRequest, sh)
		r, err := encode(templates[i], gzipLevel)
		if err != nil {
			return nil, err
		}
		raw += r.rawBytes
		gz += r.gzipBytes
	}
	log.Printf("built %d requests: %.1f KB uncompressed, %.1f KB gzip on average (%.1fx)",
		len(templates), float64(raw)/float64(len(templates))/1e3, float64(gz)/float64(len(templates))/1e3,
		float64(raw)/float64(gz))
	return templates, nil
}

type shares struct {
	traces, metrics, audit float64
}

func build(rng *rand.Rand, texts []string, records, ids, idsPerRequest int, sh shares) template {
	chosen := make([]string, 0, idsPerRequest)
	for len(chosen) < min(idsPerRequest, ids) {
		id := resourceID(rng.IntN(ids))
		if !slices.Contains(chosen, id) {
			chosen = append(chosen, id)
		}
	}
	resource := &resourcepb.Resource{Attributes: []*commonpb.KeyValue{str("service.name", "telemetry-router")}}
	scope := &commonpb.InstrumentationScope{Name: "loadgen"}

	switch kind := rng.Float64(); {
	case kind < sh.traces:
		traceID := randomBytes(rng, 16)
		spans := make([]*tracepb.Span, records)
		for i := range spans {
			spans[i] = &tracepb.Span{
				TraceId: traceID, SpanId: randomBytes(rng, 8), Name: fmt.Sprintf("operation-%d", rng.IntN(20)),
				// The duration; send moves it to the current time.
				EndTimeUnixNano: rng.Uint64N(uint64(time.Second)),
				Attributes: []*commonpb.KeyValue{
					str("stackit.resource.id", chosen[i%len(chosen)]),
					str("stackit.visibility", "PUBLIC"),
					str("payload", texts[rng.IntN(len(texts))]),
				},
			}
		}
		return template{path: "/v1/traces", records: records, msg: &coltracepb.ExportTraceServiceRequest{
			ResourceSpans: []*tracepb.ResourceSpans{{
				Resource: resource, ScopeSpans: []*tracepb.ScopeSpans{{Scope: scope, Spans: spans}},
			}},
		}}
	case kind < sh.traces+sh.metrics:
		metrics := make([]*metricspb.Metric, records)
		for i := range metrics {
			metrics[i] = &metricspb.Metric{Name: "external_egress", Unit: "By", Data: &metricspb.Metric_Gauge{
				Gauge: &metricspb.Gauge{DataPoints: []*metricspb.NumberDataPoint{{
					Value: &metricspb.NumberDataPoint_AsDouble{AsDouble: rng.Float64() * 1e6},
					Attributes: []*commonpb.KeyValue{
						str("stackit.resource.id", chosen[i%len(chosen)]),
						str("stackit.visibility", "PUBLIC"),
					},
				}}},
			}}
		}
		return template{path: "/v1/metrics", records: records, msg: &colmetricspb.ExportMetricsServiceRequest{
			ResourceMetrics: []*metricspb.ResourceMetrics{{
				Resource: resource, ScopeMetrics: []*metricspb.ScopeMetrics{{Scope: scope, Metrics: metrics}},
			}},
		}}
	default:
		logRecords := make([]*logspb.LogRecord, records)
		for i := range logRecords {
			logType := "SERVICE"
			if rng.Float64() < sh.audit {
				logType = "AUDIT"
			}
			logRecords[i] = &logspb.LogRecord{
				SeverityText:   "INFO",
				SeverityNumber: logspb.SeverityNumber_SEVERITY_NUMBER_INFO,
				Body: &commonpb.AnyValue{
					Value: &commonpb.AnyValue_StringValue{StringValue: texts[rng.IntN(len(texts))]},
				},
				Attributes: []*commonpb.KeyValue{
					str("stackit.resource.id", chosen[i%len(chosen)]),
					str("stackit.log.type", logType),
				},
			}
		}
		return template{path: "/v1/logs", records: records, msg: &collogspb.ExportLogsServiceRequest{
			ResourceLogs: []*logspb.ResourceLogs{{
				Resource: resource, ScopeLogs: []*logspb.ScopeLogs{{Scope: scope, LogRecords: logRecords}},
			}},
		}}
	}
}

// encode stamps a copy of the template with the current time and encodes it
// like an OpenTelemetry exporter: protobuf, gzip.
func encode(t template, gzipLevel int) (request, error) {
	msg := proto.Clone(t.msg)
	stamp(msg, uint64(time.Now().UnixNano()))

	raw, err := proto.Marshal(msg)
	if err != nil {
		return request{}, err
	}
	var buf bytes.Buffer
	zw, err := gzip.NewWriterLevel(&buf, gzipLevel)
	if err != nil {
		return request{}, err
	}
	if _, err = zw.Write(raw); err != nil {
		return request{}, err
	}
	if err = zw.Close(); err != nil {
		return request{}, err
	}
	return request{path: t.path, body: buf.Bytes(), records: t.records, rawBytes: len(raw), gzipBytes: buf.Len()}, nil
}

// stamp sets the timestamps of every record to now; spans keep the duration
// that build stored in EndTimeUnixNano.
func stamp(msg proto.Message, now uint64) {
	switch m := msg.(type) {
	case *collogspb.ExportLogsServiceRequest:
		stampLogs(m, now)
	case *coltracepb.ExportTraceServiceRequest:
		stampSpans(m, now)
	case *colmetricspb.ExportMetricsServiceRequest:
		stampMetrics(m, now)
	}
}

func stampLogs(m *collogspb.ExportLogsServiceRequest, now uint64) {
	for _, rl := range m.GetResourceLogs() {
		for _, sl := range rl.GetScopeLogs() {
			for _, lr := range sl.GetLogRecords() {
				lr.TimeUnixNano, lr.ObservedTimeUnixNano = now, now
			}
		}
	}
}

func stampSpans(m *coltracepb.ExportTraceServiceRequest, now uint64) {
	for _, rs := range m.GetResourceSpans() {
		for _, ss := range rs.GetScopeSpans() {
			for _, span := range ss.GetSpans() {
				span.StartTimeUnixNano, span.EndTimeUnixNano = now-span.GetEndTimeUnixNano(), now
			}
		}
	}
}

func stampMetrics(m *colmetricspb.ExportMetricsServiceRequest, now uint64) {
	for _, rm := range m.GetResourceMetrics() {
		for _, sm := range rm.GetScopeMetrics() {
			for _, metric := range sm.GetMetrics() {
				for _, dp := range metric.GetGauge().GetDataPoints() {
					dp.TimeUnixNano = now
				}
			}
		}
	}
}

// corpus builds texts of about size bytes from log lines that repeat
// templates and words, so gzip compresses them about as well as real logs
// (about 5x).
func corpus(rng *rand.Rand, n, size int) []string {
	const letters = "abcdefghijklmnopqrstuvwxyz"
	word := func() string {
		w := make([]byte, 3+rng.IntN(8))
		for j := range w {
			w[j] = letters[rng.IntN(len(letters))]
		}
		return string(w)
	}
	words := make([]string, 600)
	for i := range words {
		words[i] = word()
	}
	templates := make([]string, 120)
	for i := range templates {
		parts := make([]string, 6+rng.IntN(8))
		for j := range parts {
			parts[j] = words[rng.IntN(len(words))]
		}
		templates[i] = strings.Join(parts, " ")
	}
	texts := make([]string, n)
	for i := range texts {
		var b strings.Builder
		for b.Len() < size {
			fmt.Fprintf(
				&b,
				"%s level=info component=%s request_id=%08x user=%s duration_ms=%d %s %s\n",
				time.Now().UTC().Format(time.RFC3339Nano),
				words[rng.IntN(20)],
				rng.Uint32(),
				words[rng.IntN(len(words))],
				rng.IntN(5000),
				templates[rng.IntN(len(templates))],
				words[rng.IntN(len(words))],
			)
		}
		texts[i] = b.String()
	}
	return texts
}

func str(key, value string) *commonpb.KeyValue {
	return &commonpb.KeyValue{
		Key:   key,
		Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: value}},
	}
}

func randomBytes(rng *rand.Rand, n int) []byte {
	b := make([]byte, 0, n+8)
	for len(b) < n {
		b = binary.LittleEndian.AppendUint64(b, rng.Uint64())
	}
	return b[:n]
}

type sendStats struct {
	sent, failed, skipped, records, gzipBytes, rawBytes atomic.Int64
	mu                                                  sync.Mutex
	latencies                                           []time.Duration
	codes                                               map[int]int
}

func (s *sendStats) do(ctx context.Context, client *http.Client, url string, r request) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url+r.path, bytes.NewReader(r.body))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Content-Encoding", "gzip")
	start := time.Now()
	resp, err := client.Do(req)
	elapsed := time.Since(start)
	code := 0
	if err == nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		code = resp.StatusCode
	} else if errors.Is(ctx.Err(), context.Canceled) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return
	}
	s.mu.Lock()
	if s.codes == nil {
		s.codes = map[int]int{}
	}
	s.codes[code]++
	s.latencies = append(s.latencies, elapsed)
	s.mu.Unlock()
	if code != http.StatusOK {
		s.failed.Add(1)
		return
	}
	s.sent.Add(1)
	s.records.Add(int64(r.records))
	s.gzipBytes.Add(int64(r.gzipBytes))
	s.rawBytes.Add(int64(r.rawBytes))
}

func (s *sendStats) report(ctx context.Context) {
	t := time.NewTicker(statsInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.print("interval")
		}
	}
}

// print logs the counts since the last call as rates per second.
func (s *sendStats) print(kind string) {
	secs := statsInterval.Seconds()
	sent, failed, skipped := s.sent.Swap(0), s.failed.Swap(0), s.skipped.Swap(0)
	recs, gz, raw := s.records.Swap(0), s.gzipBytes.Swap(0), s.rawBytes.Swap(0)
	s.mu.Lock()
	lat, codes := s.latencies, s.codes
	s.latencies, s.codes = nil, nil
	s.mu.Unlock()
	slices.Sort(lat)
	q := func(p float64) float64 {
		if len(lat) == 0 {
			return 0
		}
		return float64(lat[min(len(lat)-1, int(p*float64(len(lat))))].Microseconds()) / 1e3
	}
	_ = stdout.Encode(obj{
		"ts":          time.Now().UTC().Format(time.RFC3339),
		"kind":        kind,
		"ok_rps":      float64(sent) / secs,
		"failed_rps":  float64(failed) / secs,
		"skipped_rps": float64(skipped) / secs,
		"records_per_s": float64(
			recs,
		) / secs,
		"gzip_mb_per_s": float64(gz) / secs / 1e6,
		"raw_mb_per_s":  float64(raw) / secs / 1e6,
		"p50_ms":        q(0.5),
		"p95_ms":        q(0.95),
		"p99_ms":        q(0.99),
		"codes":         codes,
	})
}

// --- receive ----------------------------------------------------------------

func receive(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("receive", flag.ExitOnError)
	listen := fs.String("listen", ":4319", "address to listen on")
	_ = fs.Parse(args)

	var requests, bytesIn atomic.Int64
	var mu sync.Mutex
	routers := map[string]int{}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/{signal}", func(w http.ResponseWriter, r *http.Request) {
		n, _ := io.Copy(io.Discard, r.Body)
		requests.Add(1)
		bytesIn.Add(n)
		mu.Lock()
		routers[r.Header.Get("X-Tlmr-Router")]++
		mu.Unlock()
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(http.StatusOK)
	})
	srv := &http.Server{Addr: *listen, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		t := time.NewTicker(statsInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				_ = srv.Close()
				return
			case <-t.C:
				mu.Lock()
				active := len(routers)
				routers = map[string]int{}
				mu.Unlock()
				_ = stdout.Encode(obj{
					"ts": time.Now().UTC().Format(time.RFC3339), "kind": "receive",
					"rps":      float64(requests.Swap(0)) / statsInterval.Seconds(),
					"mb_per_s": float64(bytesIn.Swap(0)) / statsInterval.Seconds() / 1e6, "active_routers": active,
				})
			}
		}
	}()
	log.Printf("receiving on %s", *listen)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
