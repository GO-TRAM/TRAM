// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package otlpgen

import (
	"encoding/binary"
	"fmt"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

type Shape struct {
	Records    int
	Resources  int
	Teams      int
	Attributes int
}

const (
	defaultAttributes = 4
	sdkBatch          = 100
	largeBatch        = 1000
	manyServices      = 10

	traceIDBytes = 16
	spanIDBytes  = 8
)

func Shapes() []Shape {
	return []Shape{
		{Records: 1, Resources: 1, Teams: 1, Attributes: defaultAttributes},
		{Records: sdkBatch, Resources: 1, Teams: 1, Attributes: defaultAttributes},
		{Records: largeBatch, Resources: 1, Teams: 1, Attributes: defaultAttributes},
		{Records: largeBatch, Resources: manyServices, Teams: 1, Attributes: defaultAttributes},
	}
}

func (s Shape) String() string {
	return fmt.Sprintf("records=%d/resources=%d/teams=%d", s.Records, s.Resources, s.Teams)
}

func (s Shape) recordsPerResource() int {
	return max(1, s.Records/max(1, s.Resources))
}

func (s Shape) resource(i int) *resourcepb.Resource {
	return &resourcepb.Resource{Attributes: []*commonpb.KeyValue{
		stringAttr("service.name", fmt.Sprintf("service-%d", i)),
	}}
}

func (s Shape) attributes(record int) []*commonpb.KeyValue {
	attrs := make([]*commonpb.KeyValue, 0, s.Attributes+1)
	attrs = append(attrs, stringAttr("team", fmt.Sprintf("team-%d", record%max(1, s.Teams))))
	for i := range s.Attributes {
		attrs = append(attrs, stringAttr(fmt.Sprintf("attr-%d", i), fmt.Sprintf("value-%d", record)))
	}
	return attrs
}

func scope() *commonpb.InstrumentationScope {
	return &commonpb.InstrumentationScope{Name: "otlpgen", Version: "1.0.0"}
}

func stringAttr(key, value string) *commonpb.KeyValue {
	return &commonpb.KeyValue{
		Key:   key,
		Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: value}},
	}
}

func Logs(s Shape) *logspb.LogsData {
	data := &logspb.LogsData{}
	record := 0
	for r := range max(1, s.Resources) {
		records := make([]*logspb.LogRecord, 0, s.recordsPerResource())
		for range s.recordsPerResource() {
			records = append(records, &logspb.LogRecord{
				TimeUnixNano:   uint64(record),
				SeverityNumber: logspb.SeverityNumber_SEVERITY_NUMBER_INFO,
				SeverityText:   "INFO",
				Body: &commonpb.AnyValue{
					Value: &commonpb.AnyValue_StringValue{StringValue: fmt.Sprintf("log message %d", record)},
				},
				Attributes: s.attributes(record),
			})
			record++
		}
		data.ResourceLogs = append(data.ResourceLogs, &logspb.ResourceLogs{
			Resource:  s.resource(r),
			ScopeLogs: []*logspb.ScopeLogs{{Scope: scope(), LogRecords: records}},
		})
	}
	return data
}

func Metrics(s Shape) *metricspb.MetricsData {
	data := &metricspb.MetricsData{}
	record := 0
	for r := range max(1, s.Resources) {
		metrics := make([]*metricspb.Metric, 0, s.recordsPerResource())
		for range s.recordsPerResource() {
			metrics = append(metrics, &metricspb.Metric{
				Name: fmt.Sprintf("metric_%d", record),
				Data: &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{DataPoints: []*metricspb.NumberDataPoint{{
					TimeUnixNano: uint64(record),
					Value:        &metricspb.NumberDataPoint_AsDouble{AsDouble: float64(record)},
					Attributes:   s.attributes(record),
				}}}},
			})
			record++
		}
		data.ResourceMetrics = append(data.ResourceMetrics, &metricspb.ResourceMetrics{
			Resource:     s.resource(r),
			ScopeMetrics: []*metricspb.ScopeMetrics{{Scope: scope(), Metrics: metrics}},
		})
	}
	return data
}

func Traces(s Shape) *tracepb.TracesData {
	data := &tracepb.TracesData{}
	record := 0
	for r := range max(1, s.Resources) {
		spans := make([]*tracepb.Span, 0, s.recordsPerResource())
		for range s.recordsPerResource() {
			spans = append(spans, &tracepb.Span{
				TraceId:           id(traceIDBytes, uint64(record)),
				SpanId:            id(spanIDBytes, uint64(record)),
				Name:              fmt.Sprintf("span-%d", record),
				Kind:              tracepb.Span_SPAN_KIND_SERVER,
				StartTimeUnixNano: uint64(record),
				EndTimeUnixNano:   uint64(record + 1),
				Attributes:        s.attributes(record),
			})
			record++
		}
		data.ResourceSpans = append(data.ResourceSpans, &tracepb.ResourceSpans{
			Resource:   s.resource(r),
			ScopeSpans: []*tracepb.ScopeSpans{{Scope: scope(), Spans: spans}},
		})
	}
	return data
}

func id(size int, n uint64) []byte {
	b := make([]byte, size)
	binary.BigEndian.PutUint64(b[size-spanIDBytes:], n)
	return b
}
