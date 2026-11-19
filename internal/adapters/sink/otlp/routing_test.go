// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package otlp_test

import (
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sort"
	"sync"
	"testing"

	"github.com/go-playground/validator/v10"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"

	"codeberg.org/eljoth/tram/internal/adapters/sink/otlp"
	"codeberg.org/eljoth/tram/internal/core/domain"
)

const targetExpression = "record.attributes.get_attr('target')"

// destination is an OTLP endpoint that records the log bodies and metric names
// it receives.
type destination struct {
	*httptest.Server

	mu       sync.Mutex
	received []string
	paths    []string
}

func newDestination(t *testing.T, status int) *destination {
	t.Helper()
	d := &destination{}
	d.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		d.mu.Lock()
		d.paths = append(d.paths, r.URL.Path)
		d.received = append(d.received, names(r.URL.Path, body)...)
		d.mu.Unlock()
		w.WriteHeader(status)
	}))
	t.Cleanup(d.Close)
	return d
}

// names returns the log bodies or metric names of an OTLP request body.
func names(path string, body []byte) []string {
	switch path {
	case "/v1/logs":
		return logBodies(body)
	case "/v1/metrics":
		return metricNames(body)
	}
	return nil
}

func logBodies(body []byte) []string {
	var out []string
	var logs logspb.LogsData
	_ = proto.Unmarshal(body, &logs)
	for _, rl := range logs.GetResourceLogs() {
		for _, sl := range rl.GetScopeLogs() {
			for _, record := range sl.GetLogRecords() {
				out = append(out, record.GetBody().GetStringValue())
			}
		}
	}
	return out
}

func metricNames(body []byte) []string {
	var out []string
	var metrics metricspb.MetricsData
	_ = proto.Unmarshal(body, &metrics)
	for _, rm := range metrics.GetResourceMetrics() {
		for _, sm := range rm.GetScopeMetrics() {
			for _, m := range sm.GetMetrics() {
				out = append(out, m.GetName())
			}
		}
	}
	return out
}

func (d *destination) got() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := append([]string(nil), d.received...)
	sort.Strings(out)
	return out
}

func (d *destination) requests() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.paths...)
}

func routingSink(t *testing.T, spec map[string]any) *otlp.OTLP {
	t.Helper()
	sink, err := otlp.New(domain.SinkSpec{Name: "routing", From: "in", Type: otlp.Type, Spec: spec},
		validator.New(), slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	t.Cleanup(func() { _ = sink.Stop() })
	return sink
}

func attribute(key, value string) *commonpb.KeyValue {
	return &commonpb.KeyValue{
		Key:   key,
		Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: value}},
	}
}

// targetedLogs returns a log record per body, sent to the target after it.
func targetedLogs(bodiesAndTargets ...string) *logspb.LogsData {
	var records []*logspb.LogRecord
	for i := 0; i+1 < len(bodiesAndTargets); i += 2 {
		records = append(records, &logspb.LogRecord{
			Body:       &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: bodiesAndTargets[i]}},
			Attributes: []*commonpb.KeyValue{attribute("target", bodiesAndTargets[i+1])},
		})
	}
	return &logspb.LogsData{
		ResourceLogs: []*logspb.ResourceLogs{{ScopeLogs: []*logspb.ScopeLogs{{LogRecords: records}}}},
	}
}

func TestNewURLExpression(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		spec map[string]any
		err  string
	}{
		{
			name: "url expression without url",
			spec: map[string]any{"url_expression": map[string]any{"logs": targetExpression}},
		},
		{name: "neither url nor url expression", spec: map[string]any{}, err: "URL"},
		{name: "url expression without signal", spec: map[string]any{"url_expression": map[string]any{}}, err: "Logs"},
		{
			name: "invalid expression",
			spec: map[string]any{"url_expression": map[string]any{"logs": "record.unknown"}},
			err:  "invalid url_expression",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			sink, err := otlp.New(domain.SinkSpec{Name: "s", From: "in", Type: otlp.Type, Spec: c.spec},
				validator.New(), slog.New(slog.DiscardHandler))
			if c.err == "" {
				require.NoError(t, err)
				require.NoError(t, sink.Stop())
				return
			}
			require.ErrorContains(t, err, c.err)
		})
	}
}

func TestNotifyRoutesEntriesByURLExpression(t *testing.T) {
	t.Parallel()

	a, b := newDestination(t, http.StatusOK), newDestination(t, http.StatusOK)
	sink := routingSink(t, map[string]any{"url_expression": map[string]any{"logs": targetExpression}})

	require.NoError(t, sink.Notify(t.Context(), &domain.Event{Logs: targetedLogs(
		"a1", a.URL, "b1", b.URL, "a2", a.URL+"/",
	)}))
	assert.ElementsMatch(t, []string{"a1", "a2"}, a.got(), "a trailing slash does not change the destination")
	assert.Equal(t, []string{"b1"}, b.got())
}

func TestNotifyRoutesMetricsPerDataPoint(t *testing.T) {
	t.Parallel()

	a, b := newDestination(t, http.StatusOK), newDestination(t, http.StatusOK)
	sink := routingSink(t, map[string]any{
		"url_expression": map[string]any{"metrics": "datapoint.attributes.get_attr('target')"},
	})

	require.NoError(t, sink.Notify(t.Context(), &domain.Event{Metrics: &metricspb.MetricsData{
		ResourceMetrics: []*metricspb.ResourceMetrics{{ScopeMetrics: []*metricspb.ScopeMetrics{{
			Metrics: []*metricspb.Metric{{
				Name: "requests",
				Data: &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{DataPoints: []*metricspb.NumberDataPoint{
					{Attributes: []*commonpb.KeyValue{attribute("target", a.URL)}},
					{Attributes: []*commonpb.KeyValue{attribute("target", b.URL)}},
				}}},
			}},
		}}}},
	}}))
	assert.Equal(t, []string{"requests"}, a.got())
	assert.Equal(t, []string{"requests"}, b.got())
}

func TestNotifySendsSignalsWithoutExpressionToURL(t *testing.T) {
	t.Parallel()

	routed, fallback := newDestination(t, http.StatusOK), newDestination(t, http.StatusOK)
	sink := routingSink(t, map[string]any{
		"url":            fallback.URL,
		"url_expression": map[string]any{"logs": targetExpression},
	})

	require.NoError(t, sink.Notify(t.Context(), &domain.Event{
		Logs:   targetedLogs("l1", routed.URL),
		Traces: &tracepb.TracesData{ResourceSpans: []*tracepb.ResourceSpans{{}}},
	}))
	assert.Equal(t, []string{"/v1/logs"}, routed.requests())
	assert.Equal(t, []string{"/v1/traces"}, fallback.requests())
}

func TestNotifySkipsSignalsWithoutDestination(t *testing.T) {
	t.Parallel()

	routed := newDestination(t, http.StatusOK)
	sink := routingSink(t, map[string]any{"url_expression": map[string]any{"logs": targetExpression}})

	require.NoError(t, sink.Notify(t.Context(), &domain.Event{
		Traces: &tracepb.TracesData{ResourceSpans: []*tracepb.ResourceSpans{{}}},
	}))
	assert.Empty(t, routed.requests())
}

func TestNotifyRejectsInvalidURL(t *testing.T) {
	t.Parallel()

	for _, target := range []string{"", "ftp://example.com", "http://", "not a url"} {
		sink := routingSink(t, map[string]any{"url_expression": map[string]any{"logs": targetExpression}})
		err := sink.Notify(t.Context(), &domain.Event{Logs: targetedLogs("l1", target)})
		require.ErrorIs(t, err, domain.ErrEventRejected, "target %q", target)
	}
}

func TestNotifyRetriesIfAnyDestinationMayRecover(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		statuses [2]int
		ok       bool
		rejected bool
	}{
		{name: "all accept", statuses: [2]int{http.StatusOK, http.StatusOK}, ok: true},
		{name: "one unavailable", statuses: [2]int{http.StatusOK, http.StatusServiceUnavailable}},
		{name: "one rejects", statuses: [2]int{http.StatusOK, http.StatusBadRequest}, rejected: true},
		{name: "one rejects, one unavailable", statuses: [2]int{http.StatusBadRequest, http.StatusServiceUnavailable}},
		{name: "all reject", statuses: [2]int{http.StatusBadRequest, http.StatusForbidden}, rejected: true},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			a, b := newDestination(t, c.statuses[0]), newDestination(t, c.statuses[1])
			sink := routingSink(t, map[string]any{"url_expression": map[string]any{"logs": targetExpression}})

			err := sink.Notify(t.Context(), &domain.Event{Logs: targetedLogs("a", a.URL, "b", b.URL)})
			if c.ok {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Equal(t, c.rejected, errors.Is(err, domain.ErrEventRejected))
			assert.Equal(t, []string{"a"}, a.got(), "every destination gets its data")
			assert.Equal(t, []string{"b"}, b.got())
		})
	}
}
