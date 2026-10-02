// Package otlpgen builds deterministic OTLP payloads of configurable size for
// tests and benchmarks.
package otlpgen

import (
	"fmt"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

// Shape describes the layout of a generated payload. Records are distributed
// evenly over resources; every resource has a single scope.
//
// Each resource carries a "service.name" attribute ("service-<i>"). Each record
// carries a "team" attribute cycling through Teams values ("team-<i>") and
// Attributes additional attributes.
type Shape struct {
	Records    int
	Resources  int
	Teams      int
	Attributes int
}

// Shapes are the payload sizes benchmarks are expected to cover: a single
// record, typical SDK batches, and a large batch from many services as an
// upstream collector would send it.
var Shapes = []Shape{
	{Records: 1, Resources: 1, Teams: 1, Attributes: 4},
	{Records: 100, Resources: 1, Teams: 1, Attributes: 4},
	{Records: 1000, Resources: 1, Teams: 1, Attributes: 4},
	{Records: 1000, Resources: 10, Teams: 1, Attributes: 4},
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
	return &commonpb.KeyValue{Key: key, Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: value}}}
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
				Body:           &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: fmt.Sprintf("log message %d", record)}},
				Attributes:     s.attributes(record),
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

// Metrics generates one gauge per record, each with a single data point.
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
				TraceId:           id(16, record),
				SpanId:            id(8, record),
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

func id(size, n int) []byte {
	b := make([]byte, size)
	for i := range min(size, 8) {
		b[size-1-i] = byte(n >> (8 * i))
	}
	return b
}
