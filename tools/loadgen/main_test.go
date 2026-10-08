package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"

	"codeberg.org/eljoth/tram/internal/adapters/configsync"
	"codeberg.org/eljoth/tram/internal/adapters/sink"
	"codeberg.org/eljoth/tram/internal/adapters/source"
	"codeberg.org/eljoth/tram/internal/adapters/stage"
	"codeberg.org/eljoth/tram/internal/adapters/stage/celvalidator"
	"codeberg.org/eljoth/tram/internal/core"
	"codeberg.org/eljoth/tram/internal/core/domain"
	"codeberg.org/eljoth/tram/internal/testutil/natstest"
)

// validatorOf builds the cel_validator that is the first stage of stream.
func validatorOf(t *testing.T, stream obj) *celvalidator.CELValidator {
	t.Helper()
	stages, ok := stream["stages"].([]obj)
	require.True(t, ok)
	spec, ok := stages[0]["spec"].(obj)
	require.True(t, ok)
	v, err := celvalidator.New(domain.StageSpec{Name: "validate", Spec: roundTrip(t, spec)}, validator.New())
	require.NoError(t, err)
	return v
}

// roundTrip passes spec through JSON, as TRAM reads it from a stream file.
func roundTrip(t *testing.T, spec obj) map[string]any {
	t.Helper()
	data, err := json.Marshal(spec)
	require.NoError(t, err)
	var out map[string]any
	require.NoError(t, json.Unmarshal(data, &out))
	return out
}

func logsWith(attrs ...*commonpb.KeyValue) *domain.Event {
	return &domain.Event{Logs: &logspb.LogsData{ResourceLogs: []*logspb.ResourceLogs{{ScopeLogs: []*logspb.ScopeLogs{{
		LogRecords: []*logspb.LogRecord{{Attributes: attrs}},
	}}}}}}
}

func spansWith(attrs ...*commonpb.KeyValue) *domain.Event {
	return &domain.Event{
		Traces: &tracepb.TracesData{ResourceSpans: []*tracepb.ResourceSpans{{ScopeSpans: []*tracepb.ScopeSpans{{
			Spans: []*tracepb.Span{{Attributes: attrs}},
		}}}}},
	}
}

func metricsWith(ms ...*metricspb.Metric) *domain.Event {
	return &domain.Event{Metrics: &metricspb.MetricsData{ResourceMetrics: []*metricspb.ResourceMetrics{{
		ScopeMetrics: []*metricspb.ScopeMetrics{{Metrics: ms}},
	}}}}
}

// metricsOfEveryType returns a metric of each type built like the load, with
// three data points of the tenants ids in turn.
func metricsOfEveryType(ids ...string) map[string]*metricspb.Metric {
	out := map[string]*metricspb.Metric{}
	for seed := uint64(0); len(out) < len(dataPointFields()); seed++ {
		m := metric(rand.New(rand.NewPCG(seed, seed)), 3, ids)
		out[metricType(m)] = m
	}
	return out
}

func metricType(m *metricspb.Metric) string {
	switch m.GetData().(type) {
	case *metricspb.Metric_Gauge:
		return "gauge"
	case *metricspb.Metric_Sum:
		return "sum"
	case *metricspb.Metric_Histogram:
		return "histogram"
	case *metricspb.Metric_ExponentialHistogram:
		return "exponential histogram"
	default:
		return "summary"
	}
}

func TestIngressValidator(t *testing.T) {
	t.Parallel()

	v := validatorOf(t, otlpToNATS(1, "5m", "none"))
	a, b := resourceID(0), resourceID(1)
	logType := func(value string) *commonpb.KeyValue { return str(logTypeAttr, value) }
	visibility := func(value string) *commonpb.KeyValue { return str(visibilityAttr, value) }

	cases := map[string]struct {
		event  *domain.Event
		accept bool
	}{
		"log":                           {event: logsWith(str(tenantAttr, a), logType("AUDIT")), accept: true},
		"log type with a dot":           {event: logsWith(str(tenantAttr, a), logType("AUDIT.x"))},
		"log type with whitespace":      {event: logsWith(str(tenantAttr, a), logType("AU DIT"))},
		"log type is a wildcard":        {event: logsWith(str(tenantAttr, a), logType("*"))},
		"empty log type":                {event: logsWith(str(tenantAttr, a), logType(""))},
		"log without type":              {event: logsWith(str(tenantAttr, a))},
		"log of an uppercase tenant":    {event: logsWith(str(tenantAttr, strings.ToUpper(a)), logType("AUDIT"))},
		"span":                          {event: spansWith(str(tenantAttr, a), visibility("PUBLIC")), accept: true},
		"span visibility with a dot":    {event: spansWith(str(tenantAttr, a), visibility("PUB.LIC"))},
		"span visibility is a wildcard": {event: spansWith(str(tenantAttr, a), visibility(">"))},
		"metric without data points": {
			event: metricsWith(&metricspb.Metric{Name: "empty", Data: &metricspb.Metric_Sum{Sum: &metricspb.Sum{}}}),
		},
		"metric without data": {event: metricsWith(&metricspb.Metric{Name: "bare"})},
		"data point visibility with dot": {
			event: metricsWith(withVisibility(metricsOfEveryType(a, b)["sum"], "PUB.LIC")),
		},
	}
	for name, m := range metricsOfEveryType(a, b) {
		cases[name+" of two tenants"] = struct {
			event  *domain.Event
			accept bool
		}{event: metricsWith(m), accept: true}
	}

	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			err := v.Notify(t.Context(), c.event)
			if c.accept {
				assert.NoError(t, err)
			} else {
				assert.ErrorIs(t, err, domain.ErrEventRejected)
			}
		})
	}
}

// withVisibility sets the visibility of the last data point of the sum m.
func withVisibility(m *metricspb.Metric, value string) *metricspb.Metric {
	points := m.GetSum().GetDataPoints()
	last := points[len(points)-1]
	for _, a := range last.GetAttributes() {
		if a.GetKey() == visibilityAttr {
			a.Value = &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: value}}
		}
	}
	return m
}

func TestLinkValidator(t *testing.T) {
	t.Parallel()

	own, other := resourceID(0), resourceID(1)
	v := validatorOf(t, link(own, routerID(0), 1, "5m", "none"))

	require.NoError(t, v.Notify(t.Context(), logsWith(str(tenantAttr, own))))
	require.NoError(t, v.Notify(t.Context(), spansWith(str(tenantAttr, own))))
	require.ErrorIs(t, v.Notify(t.Context(), logsWith(str(tenantAttr, other))), domain.ErrEventRejected)
	require.ErrorIs(t, v.Notify(t.Context(), spansWith(str(tenantAttr, other))), domain.ErrEventRejected)

	for name, m := range metricsOfEveryType(own) {
		require.NoError(t, v.Notify(t.Context(), metricsWith(m)), "%s of its own tenant", name)
	}
	for name, m := range metricsOfEveryType(own, other) {
		assert.ErrorIs(t, v.Notify(t.Context(), metricsWith(m)), domain.ErrEventRejected,
			"%s with a data point of another tenant", name)
	}
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// TestIngressRoutesPerDataPoint runs the generated ingress stream against NATS
// and checks that every message holds data of the tenant in its subject only.
func TestIngressRoutesPerDataPoint(t *testing.T) {
	t.Parallel()

	url := natstest.Start(t)
	port := freePort(t)
	data, err := json.Marshal(obj{"stream": otlpToNATS(1<<30, "5m", "none")})
	require.NoError(t, err)
	text := strings.ReplaceAll(string(data), "nats://nats:4222", url)
	text = strings.ReplaceAll(text, `"http_port":4318`, fmt.Sprintf(`"http_port":%d`, port))
	var stream configsync.Stream
	require.NoError(t, json.Unmarshal([]byte(text), &stream))
	stream.Stream.Name = "otlp_to_nats"

	logger := slog.New(slog.DiscardHandler)
	c := core.New(context.Background(), logger, source.NewFactory(logger), stage.NewFactory(), sink.NewFactory(logger))
	t.Cleanup(func() { _ = c.Close() })
	_, err = c.UpsertStream(context.Background(), *stream.AsDomain())
	require.NoError(t, err)

	ids := []string{resourceID(0), resourceID(1), resourceID(2)}
	var sent int
	for _, m := range metricsOfEveryType(ids...) {
		body, marshalErr := proto.Marshal(
			&colmetricspb.ExportMetricsServiceRequest{ResourceMetrics: metricsWith(m).Metrics.GetResourceMetrics()},
		)
		require.NoError(t, marshalErr)
		resp, postErr := http.Post(
			fmt.Sprintf("http://127.0.0.1:%d/v1/metrics", port),
			"application/x-protobuf",
			bytes.NewReader(body),
		)
		require.NoError(t, postErr)
		require.NoError(t, resp.Body.Close())
		require.Equal(t, http.StatusOK, resp.StatusCode, metricType(m))
		sent += 3
	}

	nc, err := nats.Connect(url)
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	require.NoError(t, err)
	telemetry, err := js.Stream(t.Context(), "telemetry")
	require.NoError(t, err)

	var routed int
	require.Eventually(t, func() bool {
		info, infoErr := telemetry.Info(t.Context())
		return infoErr == nil && info.State.Msgs > 0
	}, 10*time.Second, 10*time.Millisecond)
	info, err := telemetry.Info(t.Context())
	require.NoError(t, err)
	for seq := info.State.FirstSeq; seq <= info.State.LastSeq; seq++ {
		msg, getErr := telemetry.GetMsg(t.Context(), seq)
		require.NoError(t, getErr)
		event, decodeErr := domain.UnmarshalOTLPData(domain.Signal(msg.Header.Get("Tram-Signal")), msg.Data)
		require.NoError(t, decodeErr)
		tokens := strings.Split(msg.Subject, ".")
		require.Len(t, tokens, 3, msg.Subject)
		assert.Equal(t, "metrics.PUBLIC", tokens[0]+"."+tokens[1])
		for _, tenant := range tenantsOf(event.Metrics) {
			assert.Equal(t, tokens[2], tenant, "message on %s holds a data point of another tenant", msg.Subject)
			routed++
		}
	}
	assert.Equal(t, sent, routed, "every data point is routed once")
}

// tenantsOf returns the tenant of every data point, of any metric type.
func tenantsOf(data *metricspb.MetricsData) []string {
	var tenants []string
	for _, rm := range data.GetResourceMetrics() {
		for _, sm := range rm.GetScopeMetrics() {
			for _, m := range sm.GetMetrics() {
				for _, attrs := range pointAttributes(m) {
					tenants = append(tenants, attrValue(attrs, tenantAttr))
				}
			}
		}
	}
	return tenants
}

// pointAttributes returns the attributes of every data point of m.
func pointAttributes(m *metricspb.Metric) [][]*commonpb.KeyValue {
	var out [][]*commonpb.KeyValue
	for _, dp := range m.GetGauge().GetDataPoints() {
		out = append(out, dp.GetAttributes())
	}
	for _, dp := range m.GetSum().GetDataPoints() {
		out = append(out, dp.GetAttributes())
	}
	for _, dp := range m.GetHistogram().GetDataPoints() {
		out = append(out, dp.GetAttributes())
	}
	for _, dp := range m.GetExponentialHistogram().GetDataPoints() {
		out = append(out, dp.GetAttributes())
	}
	for _, dp := range m.GetSummary().GetDataPoints() {
		out = append(out, dp.GetAttributes())
	}
	return out
}

func attrValue(attrs []*commonpb.KeyValue, key string) string {
	for _, a := range attrs {
		if a.GetKey() == key {
			return a.GetValue().GetStringValue()
		}
	}
	return ""
}
