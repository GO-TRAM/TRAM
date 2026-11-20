// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package otlp_test

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"

	"github.com/go-tram/tram/internal/adapters/source/otlp"
	"github.com/go-tram/tram/internal/core/domain"
)

const tenantIDKey = "tenant.id"

func kv(key, value string) *commonpb.KeyValue {
	return &commonpb.KeyValue{
		Key:   key,
		Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: value}},
	}
}

func kvlist(key string, values ...*commonpb.KeyValue) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: key, Value: kvlistValue(values...)}
}

func kvlistValue(values ...*commonpb.KeyValue) *commonpb.AnyValue {
	return &commonpb.AnyValue{Value: &commonpb.AnyValue_KvlistValue{
		KvlistValue: &commonpb.KeyValueList{Values: values},
	}}
}

// distinct returns n attributes with different keys.
func distinct(n int) []*commonpb.KeyValue {
	attrs := make([]*commonpb.KeyValue, n)
	for i := range attrs {
		attrs[i] = kv(fmt.Sprintf("key-%d", i), "value")
	}
	return attrs
}

func twice(key string) []*commonpb.KeyValue {
	return []*commonpb.KeyValue{kv(key, "A"), kv("other", "x"), kv(key, "B")}
}

type logsParts struct {
	resource, scope, record []*commonpb.KeyValue
	body                    *commonpb.AnyValue
}

func logsEvent(p logsParts) *domain.Event {
	return &domain.Event{Logs: &logspb.LogsData{ResourceLogs: []*logspb.ResourceLogs{{
		Resource: &resourcepb.Resource{Attributes: p.resource},
		ScopeLogs: []*logspb.ScopeLogs{{
			Scope:      &commonpb.InstrumentationScope{Attributes: p.scope},
			LogRecords: []*logspb.LogRecord{{Attributes: kvs("record", "1")}, {Attributes: p.record, Body: p.body}},
		}},
	}}}}
}

func kvs(key, value string) []*commonpb.KeyValue { return []*commonpb.KeyValue{kv(key, value)} }

func metricsEvent(metric *metricspb.Metric) *domain.Event {
	return &domain.Event{Metrics: &metricspb.MetricsData{ResourceMetrics: []*metricspb.ResourceMetrics{{
		ScopeMetrics: []*metricspb.ScopeMetrics{{Metrics: []*metricspb.Metric{metric}}},
	}}}}
}

func tracesEvent(span *tracepb.Span) *domain.Event {
	return &domain.Event{Traces: &tracepb.TracesData{ResourceSpans: []*tracepb.ResourceSpans{{
		ScopeSpans: []*tracepb.ScopeSpans{{Spans: []*tracepb.Span{span}}},
	}}}}
}

func numberPoints(attrs []*commonpb.KeyValue) []*metricspb.NumberDataPoint {
	return []*metricspb.NumberDataPoint{{Attributes: kvs("point", "1")}, {Attributes: attrs}}
}

func TestDuplicateKey(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		event     *domain.Event
		duplicate string
	}{
		{
			name:  "unique keys",
			event: logsEvent(logsParts{resource: distinct(3), scope: distinct(3), record: distinct(3)}),
		},
		{
			name: "same key in different lists",
			event: logsEvent(
				logsParts{resource: kvs(tenantIDKey, "A"), scope: kvs(tenantIDKey, "A"), record: kvs(tenantIDKey, "B")},
			),
		},
		{name: "empty event", event: &domain.Event{}},
		{name: "record", event: logsEvent(logsParts{record: twice(tenantIDKey)}), duplicate: tenantIDKey},
		{name: "resource", event: logsEvent(logsParts{resource: twice("service.name")}), duplicate: "service.name"},
		{name: "scope", event: logsEvent(logsParts{scope: twice("library")}), duplicate: "library"},
		{
			name:  "longer list without duplicate",
			event: logsEvent(logsParts{record: distinct(otlp.MaxPairwiseKeys + 4)}),
		},
		{
			name: "longer list",
			event: logsEvent(logsParts{
				record: append(distinct(otlp.MaxPairwiseKeys+4), kv("key-7", "again")),
			}),
			duplicate: "key-7",
		},
		{
			name:      "key-value list in an attribute",
			event:     logsEvent(logsParts{record: []*commonpb.KeyValue{kvlist("http", twice("method")...)}}),
			duplicate: "method",
		},
		{
			name:  "same key in an attribute and its key-value list",
			event: logsEvent(logsParts{record: []*commonpb.KeyValue{kvlist("http", kv("http", "nested"))}}),
		},
		{
			name: "key-value list in an array",
			event: logsEvent(logsParts{record: []*commonpb.KeyValue{{Key: "items", Value: &commonpb.AnyValue{
				Value: &commonpb.AnyValue_ArrayValue{ArrayValue: &commonpb.ArrayValue{
					Values: []*commonpb.AnyValue{kvlistValue(kv("id", "1")), kvlistValue(twice("id")...)},
				}},
			}}}}),
			duplicate: "id",
		},
		{name: "log body", event: logsEvent(logsParts{body: kvlistValue(twice("message")...)}), duplicate: "message"},
		{
			name: "gauge data point",
			event: metricsEvent(
				&metricspb.Metric{
					Data: &metricspb.Metric_Gauge{
						Gauge: &metricspb.Gauge{DataPoints: numberPoints(twice(tenantIDKey))},
					},
				},
			),
			duplicate: tenantIDKey,
		},
		{
			name: "sum data point",
			event: metricsEvent(
				&metricspb.Metric{
					Data: &metricspb.Metric_Sum{Sum: &metricspb.Sum{DataPoints: numberPoints(twice(tenantIDKey))}},
				},
			),
			duplicate: tenantIDKey,
		},
		{
			name: "histogram data point",
			event: metricsEvent(&metricspb.Metric{Data: &metricspb.Metric_Histogram{Histogram: &metricspb.Histogram{
				DataPoints: []*metricspb.HistogramDataPoint{{Attributes: twice(tenantIDKey)}},
			}}}),
			duplicate: tenantIDKey,
		},
		{
			name: "exponential histogram data point",
			event: metricsEvent(&metricspb.Metric{Data: &metricspb.Metric_ExponentialHistogram{
				ExponentialHistogram: &metricspb.ExponentialHistogram{
					DataPoints: []*metricspb.ExponentialHistogramDataPoint{{Attributes: twice(tenantIDKey)}},
				},
			}}),
			duplicate: tenantIDKey,
		},
		{
			name: "summary data point",
			event: metricsEvent(&metricspb.Metric{Data: &metricspb.Metric_Summary{Summary: &metricspb.Summary{
				DataPoints: []*metricspb.SummaryDataPoint{{Attributes: twice(tenantIDKey)}},
			}}}),
			duplicate: tenantIDKey,
		},
		{
			name: "exemplar",
			event: metricsEvent(&metricspb.Metric{Data: &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{
				DataPoints: []*metricspb.NumberDataPoint{
					{Exemplars: []*metricspb.Exemplar{{FilteredAttributes: twice("trace")}}},
				},
			}}}),
			duplicate: "trace",
		},
		{name: "metric metadata", event: metricsEvent(&metricspb.Metric{Metadata: twice("unit")}), duplicate: "unit"},
		{name: "span", event: tracesEvent(&tracepb.Span{Attributes: twice(tenantIDKey)}), duplicate: tenantIDKey},
		{
			name:      "span event",
			event:     tracesEvent(&tracepb.Span{Events: []*tracepb.Span_Event{{Attributes: twice("exception.type")}}}),
			duplicate: "exception.type",
		},
		{
			name:      "span link",
			event:     tracesEvent(&tracepb.Span{Links: []*tracepb.Span_Link{{Attributes: twice("link.kind")}}}),
			duplicate: "link.kind",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			key, found := otlp.DuplicateKey(c.event)
			assert.Equal(t, c.duplicate != "", found)
			assert.Equal(t, c.duplicate, key)
		})
	}
}

func TestDuplicateAttributeIsRejected(t *testing.T) {
	t.Parallel()

	body, err := proto.Marshal(logsEvent(logsParts{record: twice(tenantIDKey)}).Logs)
	require.NoError(t, err)

	o, rec := newSource(t, nil)
	w := httptest.NewRecorder()
	otlp.HandleLogs(o, w, httptest.NewRequest(http.MethodPost, "/v1/logs", bytes.NewReader(body)))

	assert.Equal(t, http.StatusUnprocessableEntity, w.Code, "the client does not retry data that is refused again")
	assert.Contains(t, w.Body.String(), `"tenant.id"`)
	assert.Empty(t, rec.events, "nothing is passed on")
}
