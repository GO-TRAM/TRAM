// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package otlp

import (
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"

	"codeberg.org/eljoth/tram/internal/core/domain"
)

// maxPairwiseKeys bounds the lists whose keys are compared pair by pair,
// which allocates nothing. Longer lists are checked with a set, since the
// pairs grow with the square of the length.
const maxPairwiseKeys = 16

// duplicateKey returns a key that appears more than once in one of the
// event's key-value lists. OTLP requires unique keys but does not enforce
// them; with a key twice, a stage could check one value while a sink routes
// by the other.
func duplicateKey(event *domain.Event) (string, bool) {
	var c keyCheck
	c.logs(event.Logs)
	c.metrics(event.Metrics)
	c.traces(event.Traces)
	return c.key, c.found
}

// keyCheck remembers the first duplicate key found; later checks do nothing.
type keyCheck struct {
	key   string
	found bool
}

func (c *keyCheck) logs(logs *logspb.LogsData) {
	for _, rl := range logs.GetResourceLogs() {
		c.attributes(rl.GetResource().GetAttributes())
		for _, sl := range rl.GetScopeLogs() {
			c.attributes(sl.GetScope().GetAttributes())
			for _, record := range sl.GetLogRecords() {
				c.attributes(record.GetAttributes())
				c.value(record.GetBody())
			}
		}
	}
}

func (c *keyCheck) metrics(metrics *metricspb.MetricsData) {
	for _, rm := range metrics.GetResourceMetrics() {
		c.attributes(rm.GetResource().GetAttributes())
		for _, sm := range rm.GetScopeMetrics() {
			c.attributes(sm.GetScope().GetAttributes())
			for _, m := range sm.GetMetrics() {
				c.attributes(m.GetMetadata())
				c.metric(m)
			}
		}
	}
}

func (c *keyCheck) traces(traces *tracepb.TracesData) {
	for _, rs := range traces.GetResourceSpans() {
		c.attributes(rs.GetResource().GetAttributes())
		for _, ss := range rs.GetScopeSpans() {
			c.attributes(ss.GetScope().GetAttributes())
			for _, span := range ss.GetSpans() {
				c.attributes(span.GetAttributes())
				for _, e := range span.GetEvents() {
					c.attributes(e.GetAttributes())
				}
				for _, l := range span.GetLinks() {
					c.attributes(l.GetAttributes())
				}
			}
		}
	}
}

func (c *keyCheck) metric(m *metricspb.Metric) {
	switch data := m.GetData().(type) {
	case *metricspb.Metric_Gauge:
		c.numberDataPoints(data.Gauge.GetDataPoints())
	case *metricspb.Metric_Sum:
		c.numberDataPoints(data.Sum.GetDataPoints())
	case *metricspb.Metric_Histogram:
		for _, dp := range data.Histogram.GetDataPoints() {
			c.attributes(dp.GetAttributes())
			c.exemplars(dp.GetExemplars())
		}
	case *metricspb.Metric_ExponentialHistogram:
		for _, dp := range data.ExponentialHistogram.GetDataPoints() {
			c.attributes(dp.GetAttributes())
			c.exemplars(dp.GetExemplars())
		}
	case *metricspb.Metric_Summary:
		for _, dp := range data.Summary.GetDataPoints() {
			c.attributes(dp.GetAttributes())
		}
	}
}

func (c *keyCheck) numberDataPoints(points []*metricspb.NumberDataPoint) {
	for _, dp := range points {
		c.attributes(dp.GetAttributes())
		c.exemplars(dp.GetExemplars())
	}
}

func (c *keyCheck) exemplars(exemplars []*metricspb.Exemplar) {
	for _, e := range exemplars {
		c.attributes(e.GetFilteredAttributes())
	}
}

// attributes checks a list and the key-value lists nested in its values.
func (c *keyCheck) attributes(list []*commonpb.KeyValue) {
	if c.found {
		return
	}
	if key, ok := repeatedKey(list); ok {
		c.key, c.found = key, true
		return
	}
	for _, kv := range list {
		c.value(kv.GetValue())
	}
}

// repeatedKey returns the first key that occurs a second time in list. Short
// lists are compared pairwise, which is faster than building a set.
func repeatedKey(list []*commonpb.KeyValue) (string, bool) {
	if len(list) <= maxPairwiseKeys {
		for i := 1; i < len(list); i++ {
			for _, earlier := range list[:i] {
				if list[i].GetKey() == earlier.GetKey() {
					return list[i].GetKey(), true
				}
			}
		}
		return "", false
	}
	seen := make(map[string]struct{}, len(list))
	for _, kv := range list {
		if _, ok := seen[kv.GetKey()]; ok {
			return kv.GetKey(), true
		}
		seen[kv.GetKey()] = struct{}{}
	}
	return "", false
}

func (c *keyCheck) value(v *commonpb.AnyValue) {
	switch v := v.GetValue().(type) {
	case *commonpb.AnyValue_KvlistValue:
		c.attributes(v.KvlistValue.GetValues())
	case *commonpb.AnyValue_ArrayValue:
		for _, element := range v.ArrayValue.GetValues() {
			c.value(element)
		}
	}
}
