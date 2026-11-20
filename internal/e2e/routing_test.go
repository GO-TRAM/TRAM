// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package e2e_test

import (
	"fmt"
	"net/http"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"

	"codeberg.org/eljoth/tram/internal/core/domain"
)

const (
	logsPath   = "/v1/logs"
	logsA1     = "logs:a1"
	logsA2     = "logs:a2"
	metricsPA1 = "metrics:mixed/pa1"
	metricsPA3 = "metrics:mixed/pa3"
	tracesSA1  = "traces:sa1"
)

// Every log record, span and data point carries the customer it belongs to.
const (
	customerAttr = "customer"
	idAttr       = "id"
)

// hasCustomer is a CEL condition on the attributes attrs: the customer is
// exactly customer, or with customer "", any value of lowercase letters.
func hasCustomer(attrs, customer string) string {
	value := "a.value.string_value.matches('^[a-z]+$')"
	if customer != "" {
		value = fmt.Sprintf("a.value.string_value == '%s'", customer)
	}
	return fmt.Sprintf("%s.exists(a, a.key == '%s' && %s)", attrs, customerAttr, value)
}

// customerValidator rejects an event unless every log record, span and data
// point (of gauges and sums) has the customer, see hasCustomer.
func customerValidator(name, from, customer string) domain.StageSpec {
	return domain.StageSpec{Name: name, Type: "validation", From: from, Spec: map[string]any{
		"driver":      "cel",
		"on_mismatch": "reject",
		"cel": map[string]any{
			"logs": "logs.resource_logs.all(r, r.scope_logs.all(s, s.log_records.all(record, " +
				hasCustomer("record.attributes", customer) + ")))",
			"traces": "traces.resource_spans.all(r, r.scope_spans.all(s, s.spans.all(span, " +
				hasCustomer("span.attributes", customer) + ")))",
			"metrics": "metrics.resource_metrics.all(r, r.scope_metrics.all(s, s.metrics.all(m, " +
				"m.gauge.data_points.all(dp, " + hasCustomer("dp.attributes", customer) + ") && " +
				"m.sum.data_points.all(dp, " + hasCustomer("dp.attributes", customer) + "))))",
		},
	}}
}

type entry struct{ id, customer string }

func entryAttrs(e entry) []*commonpb.KeyValue {
	return []*commonpb.KeyValue{stringAttr(idAttr, e.id), stringAttr(customerAttr, e.customer)}
}

func spans(entries ...entry) *tracepb.TracesData {
	ss := &tracepb.ScopeSpans{Scope: &commonpb.InstrumentationScope{Name: "e2e"}}
	for _, e := range entries {
		ss.Spans = append(ss.Spans, &tracepb.Span{Name: e.id, Attributes: entryAttrs(e)})
	}
	return &tracepb.TracesData{ResourceSpans: []*tracepb.ResourceSpans{{ScopeSpans: []*tracepb.ScopeSpans{ss}}}}
}

func points(entries ...entry) []*metricspb.NumberDataPoint {
	out := make([]*metricspb.NumberDataPoint, len(entries))
	for i, e := range entries {
		out[i] = &metricspb.NumberDataPoint{Attributes: entryAttrs(e)}
	}
	return out
}

func metrics(ms ...*metricspb.Metric) *metricspb.MetricsData {
	return &metricspb.MetricsData{ResourceMetrics: []*metricspb.ResourceMetrics{{
		ScopeMetrics: []*metricspb.ScopeMetrics{{Scope: &commonpb.InstrumentationScope{Name: "e2e"}, Metrics: ms}},
	}}}
}

// receivedEntries lists what a sink received, sorted, as signal:id, with data
// points as metrics:metric/id.
func receivedEntries(events []*domain.Event) []string {
	var out []string
	for _, e := range events {
		out = appendLogEntries(out, e.Logs)
		out = appendSpanEntries(out, e.Traces)
		out = appendDataPointEntries(out, e.Metrics)
	}
	sort.Strings(out)
	return out
}

func appendLogEntries(out []string, data *logspb.LogsData) []string {
	for _, rl := range data.GetResourceLogs() {
		for _, sl := range rl.GetScopeLogs() {
			for _, r := range sl.GetLogRecords() {
				out = append(out, "logs:"+r.GetBody().GetStringValue())
			}
		}
	}
	return out
}

func appendSpanEntries(out []string, data *tracepb.TracesData) []string {
	for _, rs := range data.GetResourceSpans() {
		for _, ss := range rs.GetScopeSpans() {
			for _, s := range ss.GetSpans() {
				out = append(out, "traces:"+s.GetName())
			}
		}
	}
	return out
}

// appendDataPointEntries lists the data points of gauges and sums.
func appendDataPointEntries(out []string, data *metricspb.MetricsData) []string {
	for _, rm := range data.GetResourceMetrics() {
		for _, sm := range rm.GetScopeMetrics() {
			for _, m := range sm.GetMetrics() {
				for _, points := range [][]*metricspb.NumberDataPoint{
					m.GetGauge().GetDataPoints(), m.GetSum().GetDataPoints(),
				} {
					for _, dp := range points {
						out = append(out, "metrics:"+m.GetName()+"/"+attrValue(dp.GetAttributes(), idAttr))
					}
				}
			}
		}
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

type post struct {
	path    string
	payload proto.Message
}

// postAll sends the requests at the same time, so they share batches, and
// returns their status codes in order.
func (p *pipeline) postAll(t *testing.T, posts ...post) []int {
	t.Helper()
	codes := make([]int, len(posts))
	var wg sync.WaitGroup
	for i, r := range posts {
		wg.Go(func() { codes[i] = p.postTo(t, r.path, r.payload, 10*time.Second) })
	}
	wg.Wait()
	return codes
}

// testRoutingByRecordAttribute routes the records of several customers that
// arrive mixed in the same requests through one NATS stream to one consumer per
// customer and signal. Each consumer validates that it only gets records of its
// customer.
func testRoutingByRecordAttribute(t *testing.T, n *natsServer) {
	t.Helper()
	stream := n.brokerStream(t, "routing")

	ingest := newPipeline(t,
		[]domain.StageSpec{
			customerValidator("validate", "in", ""),
			{Name: "batch", Type: "batch", From: "validate", Spec: map[string]any{
				"max_records": float64(1000), "timeout": "200ms",
			}},
		},
		[]domain.SinkSpec{{Name: "out", Type: "broker", From: "batch", Spec: n.brokerSinkSpec(map[string]any{
			"logs":    fmt.Sprintf("'%s.logs.' + record.attributes.get_attr('%s')", stream, customerAttr),
			"traces":  fmt.Sprintf("'%s.traces.' + span.attributes.get_attr('%s')", stream, customerAttr),
			"metrics": fmt.Sprintf("'%s.metrics.' + datapoint.attributes.get_attr('%s')", stream, customerAttr),
		})}})

	// One source per customer and signal, since a source reads one signal.
	consumers := map[string]customerSinks{}
	for _, customer := range []string{"a", "b"} {
		for _, signal := range []string{"logs", "traces", "metrics"} {
			consumer := newStream(t, n.brokerSource(stream, signal, stream+"."+signal+"."+customer),
				[]domain.StageSpec{customerValidator("only-"+customer, "in", customer)},
				[]domain.SinkSpec{captureSinkSpec("received", "only-"+customer)})
			consumers[customer] = append(consumers[customer], consumer.sinks.sink(t, "received"))
		}
	}

	// expect waits until each consumer received exactly its entries, and
	// checks that nothing else arrives shortly after.
	expect := func(t *testing.T, want map[string][]string) {
		t.Helper()
		for customer, entries := range want {
			sort.Strings(entries)
			require.Eventually(t, func() bool {
				return len(receivedEntries(consumers[customer].received())) >= len(entries)
			}, 10*time.Second, 10*time.Millisecond, "customer %s did not receive its records", customer)
		}
		time.Sleep(300 * time.Millisecond)
		for customer, entries := range want {
			assert.Equal(t, entries, receivedEntries(consumers[customer].received()), "customer %s", customer)
		}
	}

	t.Run("each consumer receives exactly the records of its customer", func(t *testing.T) {
		codes := ingest.postAll(t,
			post{logsPath, request("shop", rec{"a1", "a"}, rec{"b1", "b"}, rec{"c1", "c"})},
			post{logsPath, request("cart", rec{"a2", "a"})},
			post{"/v1/traces", spans(entry{"sb1", "b"}, entry{"sa1", "a"}, entry{"sc1", "c"})},
			post{"/v1/metrics", metrics(
				&metricspb.Metric{Name: "mixed", Data: &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{
					DataPoints: points(entry{"pa1", "a"}, entry{"pb2", "b"}, entry{"pa3", "a"}, entry{"pc4", "c"}),
				}}},
				&metricspb.Metric{Name: "total", Data: &metricspb.Metric_Sum{Sum: &metricspb.Sum{
					DataPoints: points(entry{"pb5", "b"}), IsMonotonic: true,
				}}},
			)},
		)
		require.Equal(t, []int{http.StatusOK, http.StatusOK, http.StatusOK, http.StatusOK}, codes)

		expect(t, map[string][]string{
			"a": {logsA1, logsA2, tracesSA1, metricsPA1, metricsPA3},
			"b": {"logs:b1", "traces:sb1", "metrics:mixed/pb2", "metrics:total/pb5"},
		})
	})

	t.Run("invalid requests fail on their own", func(t *testing.T) {
		duplicate := request("shop", rec{"dup", "a"})
		record := duplicate.GetResourceLogs()[0].GetScopeLogs()[0].GetLogRecords()[0]
		record.Attributes = append(record.Attributes, stringAttr(customerAttr, "b"))

		codes := ingest.postAll(t,
			post{logsPath, request("shop", rec{"a3", "a"})},
			post{logsPath, request("shop", rec{"bad", "B!"})},
			post{logsPath, duplicate},
		)
		assert.Equal(t, []int{http.StatusOK, http.StatusUnprocessableEntity, http.StatusUnprocessableEntity}, codes,
			"an invalid customer and a customer given twice are refused, the valid request is delivered")

		expect(t, map[string][]string{
			"a": {logsA1, logsA2, "logs:a3", tracesSA1, metricsPA1, metricsPA3},
		})
	})

	t.Run("a consumer rejects records of other customers on its subject", func(t *testing.T) {
		publish := func(logs *logspb.LogsData) {
			data, err := proto.Marshal(logs)
			require.NoError(t, err)
			_, err = n.js.PublishMsg(t.Context(), &nats.Msg{Subject: stream + ".logs.a", Data: data})
			require.NoError(t, err)
		}
		publish(request("intruder", rec{"foreign", "b"}))
		publish(request("intruder", rec{"own", "a"}, rec{"mixed", "b"}))

		cons, err := n.js.Consumer(t.Context(), stream, stream+"_logs_a")
		require.NoError(t, err)
		require.Eventually(t, func() bool {
			info, infoErr := cons.Info(t.Context())
			return infoErr == nil && info.NumPending == 0 && info.NumAckPending == 0
		}, 10*time.Second, 10*time.Millisecond, "the messages were not handled")

		assert.Equal(t,
			[]string{logsA1, logsA2, "logs:a3", metricsPA1, metricsPA3, tracesSA1},
			receivedEntries(consumers["a"].received()),
			"neither the foreign message nor the mixed one reaches the sink")
	})
}

// customerSinks are the capture sinks of one customer's sources.
type customerSinks []*captureSink

func (c customerSinks) received() []*domain.Event {
	var events []*domain.Event
	for _, s := range c {
		events = append(events, s.received()...)
	}
	return events
}
