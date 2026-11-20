// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package core_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"codeberg.org/tram/tram/internal/core"
	"codeberg.org/tram/tram/internal/core/domain"
	"codeberg.org/tram/tram/internal/testutil/otlpgen"
)

const (
	recordsMetric    = "tram.node.records"
	durationMetric   = "tram.node.duration"
	recordsOutMetric = "tram.node.records.out"
)

func newMeteredCore(t *testing.T) (*core.Core, *registry, *sdkmetric.ManualReader) {
	t.Helper()
	c, reg := newCore(t)
	reader := sdkmetric.NewManualReader()
	require.NoError(t, c.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))))
	return c, reg, reader
}

func collect(t *testing.T, reader *sdkmetric.ManualReader, name string) map[attribute.Distinct]int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))

	points := map[attribute.Distinct]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == name {
				addPoints(points, m.Data)
			}
		}
	}
	return points
}

// addPoints adds the values of counters and the counts of histograms.
func addPoints(points map[attribute.Distinct]int64, data metricdata.Aggregation) {
	switch data := data.(type) {
	case metricdata.Sum[int64]:
		for _, dp := range data.DataPoints {
			points[dp.Attributes.Equivalent()] = dp.Value
		}
	case metricdata.Histogram[float64]:
		for _, dp := range data.DataPoints {
			points[dp.Attributes.Equivalent()] = int64(dp.Count)
		}
	}
}

func attrs(node, kind string, kv ...attribute.KeyValue) attribute.Distinct {
	set := attribute.NewSet(append([]attribute.KeyValue{
		attribute.String("stream", "stream"),
		attribute.String("node", node),
		attribute.String("kind", kind),
	}, kv...)...)
	return set.Equivalent()
}

func signal(s domain.Signal) attribute.KeyValue { return attribute.String("signal", string(s)) }
func outcome(o string) attribute.KeyValue       { return attribute.String("outcome", o) }

func deliver(t *testing.T, reg *registry, event *domain.Event) error {
	t.Helper()
	subs := reg.sources[len(reg.sources)-1].subs
	require.Len(t, subs, 1, "a source has a single subscriber, its fan-out")
	return subs[0].Notify(t.Context(), event)
}

func logs(records int) *domain.Event {
	return &domain.Event{Logs: otlpgen.Logs(otlpgen.Shape{Records: records, Resources: 1, Teams: 1})}
}

func TestMetricsCountRecordsPerSignal(t *testing.T) {
	t.Parallel()

	c, reg, reader := newMeteredCore(t)
	_, err := c.UpsertStream(t.Context(), streamSpec(1, sink("sink", "fake")))
	require.NoError(t, err)

	event := &domain.Event{
		Logs:   otlpgen.Logs(otlpgen.Shape{Records: 2, Resources: 1, Teams: 1}),
		Traces: otlpgen.Traces(otlpgen.Shape{Records: 3, Resources: 1, Teams: 1}),
	}
	require.NoError(t, deliver(t, reg, event))
	require.NoError(t, deliver(t, reg, event))

	assert.Equal(t, map[attribute.Distinct]int64{
		attrs("source", "source", signal(domain.SignalLogs)):   4,
		attrs("source", "source", signal(domain.SignalTraces)): 6,
		attrs("sink", "sink", signal(domain.SignalLogs)):       4,
		attrs("sink", "sink", signal(domain.SignalTraces)):     6,
	}, collect(t, reader, recordsMetric), "signals without records are left out")
	assert.Equal(t, map[attribute.Distinct]int64{
		attrs("source", "source", outcome("ok")): 2,
		attrs("sink", "sink", outcome("ok")):     2,
	}, collect(t, reader, durationMetric))
}

func TestMetricsCountSourceOncePerEvent(t *testing.T) {
	t.Parallel()

	c, reg, reader := newMeteredCore(t)
	_, err := c.UpsertStream(t.Context(), streamSpec(1, sink("a", "fake"), sink("b", "fake"), sink("c", "fake")))
	require.NoError(t, err)

	require.NoError(t, deliver(t, reg, logs(2)))

	assert.Equal(t, map[attribute.Distinct]int64{
		attrs("source", "source", signal(domain.SignalLogs)): 2,
		attrs("a", "sink", signal(domain.SignalLogs)):        2,
		attrs("b", "sink", signal(domain.SignalLogs)):        2,
		attrs("c", "sink", signal(domain.SignalLogs)):        2,
	}, collect(t, reader, recordsMetric))
}

func TestMetricsRecordOutcome(t *testing.T) {
	t.Parallel()

	cases := []struct {
		sinkType string
		outcome  string
	}{
		{sinkType: rejectingType, outcome: "rejected"},
		{sinkType: failingType, outcome: "error"},
	}

	for _, tc := range cases {
		t.Run(tc.outcome, func(t *testing.T) {
			t.Parallel()

			c, reg, reader := newMeteredCore(t)
			_, err := c.UpsertStream(t.Context(), streamSpec(1, sink("ok", "fake"), sink("broken", tc.sinkType)))
			require.NoError(t, err)

			require.Error(t, deliver(t, reg, logs(1)))

			assert.Equal(t, map[attribute.Distinct]int64{
				attrs("source", "source", outcome(tc.outcome)): 1,
				attrs("ok", "sink", outcome("ok")):             1,
				attrs("broken", "sink", outcome(tc.outcome)):   1,
			}, collect(t, reader, durationMetric))
		})
	}
}

func stagedSpec(metrics domain.MetricsSpec) domain.StreamSpec {
	spec := streamSpec(1, domain.SinkSpec{Name: "sink", Type: "fake", From: "stage"})
	spec.Stages = []domain.StageSpec{{Name: "stage", Type: forwardingType, From: "source"}}
	spec.Metrics = metrics
	return spec
}

func TestMetricsLeaveOutStagesByDefault(t *testing.T) {
	t.Parallel()

	c, reg, reader := newMeteredCore(t)
	_, err := c.UpsertStream(t.Context(), stagedSpec(domain.MetricsSpec{}))
	require.NoError(t, err)

	require.NoError(t, deliver(t, reg, logs(1)))

	assert.Equal(t, map[attribute.Distinct]int64{
		attrs("source", "source", signal(domain.SignalLogs)): 1,
		attrs("sink", "sink", signal(domain.SignalLogs)):     1,
	}, collect(t, reader, recordsMetric))
	assert.Equal(t, map[attribute.Distinct]int64{
		attrs("source", "source", signal(domain.SignalLogs)): 1,
	}, collect(t, reader, recordsOutMetric))
	assert.Equal(t, map[attribute.Distinct]int64{
		attrs("source", "source", outcome("ok")): 1,
		attrs("sink", "sink", outcome("ok")):     1,
	}, collect(t, reader, durationMetric))
}

func TestMetricsCoverStagesWhenEnabled(t *testing.T) {
	t.Parallel()

	c, reg, reader := newMeteredCore(t)
	_, err := c.UpsertStream(t.Context(), stagedSpec(domain.MetricsSpec{
		Records:  domain.NodeMetrics{Stages: new(true)},
		Duration: domain.NodeMetrics{Stages: new(true)},
	}))
	require.NoError(t, err)

	require.NoError(t, deliver(t, reg, logs(1)))

	assert.Equal(t, map[attribute.Distinct]int64{
		attrs("source", "source", signal(domain.SignalLogs)): 1,
		attrs("stage", "stage", signal(domain.SignalLogs)):   1,
		attrs("sink", "sink", signal(domain.SignalLogs)):     1,
	}, collect(t, reader, recordsMetric))
	assert.Equal(t, map[attribute.Distinct]int64{
		attrs("source", "source", outcome("ok")): 1,
		attrs("stage", "stage", outcome("ok")):   1,
		attrs("sink", "sink", outcome("ok")):     1,
	}, collect(t, reader, durationMetric))
}

func TestMetricsLeaveOutDisabledMetrics(t *testing.T) {
	t.Parallel()

	c, reg, reader := newMeteredCore(t)
	_, err := c.UpsertStream(t.Context(), stagedSpec(domain.MetricsSpec{
		Records:     domain.NodeMetrics{Source: new(false)},
		Duration:    domain.NodeMetrics{Sinks: new(false)},
		RequestSize: new(false),
	}))
	require.NoError(t, err)

	event := logs(1)
	event.Request = &domain.Request{Encoding: "gzip", WireBytes: 100, DecodedBytes: 500}
	require.NoError(t, deliver(t, reg, event))

	assert.Equal(t, map[attribute.Distinct]int64{
		attrs("sink", "sink", signal(domain.SignalLogs)): 1,
	}, collect(t, reader, recordsMetric))
	assert.Empty(t, collect(t, reader, recordsOutMetric), "records.out follows records")
	assert.Equal(t, map[attribute.Distinct]int64{
		attrs("source", "source", outcome("ok")): 1,
	}, collect(t, reader, durationMetric))
	assert.Empty(t, requestSizes(t, reader))
}

func TestMetricsFollowChangedSettings(t *testing.T) {
	t.Parallel()

	c, reg, reader := newMeteredCore(t)
	_, err := c.UpsertStream(t.Context(), stagedSpec(domain.MetricsSpec{}))
	require.NoError(t, err)

	_, err = c.UpsertStream(t.Context(), stagedSpec(domain.MetricsSpec{Records: domain.NodeMetrics{Stages: new(true)}}))
	require.NoError(t, err)
	require.Equal(t, 1, reg.sourceCount(), "a change of the metrics keeps the source running")

	require.NoError(t, deliver(t, reg, logs(1)))

	assert.Contains(t, collect(t, reader, recordsMetric),
		attrs("stage", "stage", signal(domain.SignalLogs)), "the stage records once enabled")
}

func TestMetricsSurviveReplacedReceivers(t *testing.T) {
	t.Parallel()

	c, reg, reader := newMeteredCore(t)
	_, err := c.UpsertStream(t.Context(), streamSpec(1, sink("old", "fake")))
	require.NoError(t, err)

	_, err = c.UpsertStream(t.Context(), streamSpec(1, sink("new", "fake")))
	require.NoError(t, err)
	require.Equal(t, 1, reg.sourceCount(), "source must keep running")

	require.NoError(t, deliver(t, reg, logs(1)))

	assert.Equal(t, map[attribute.Distinct]int64{
		attrs("source", "source", signal(domain.SignalLogs)): 1,
		attrs("new", "sink", signal(domain.SignalLogs)):      1,
	}, collect(t, reader, recordsMetric))
}

func TestMetricsCountRecordsOut(t *testing.T) {
	t.Parallel()

	c, reg, reader := newMeteredCore(t)
	spec := streamSpec(1,
		domain.SinkSpec{Name: "a", Type: "fake", From: "forward"},
		domain.SinkSpec{Name: "b", Type: "fake", From: "forward"},
		domain.SinkSpec{Name: "c", Type: "fake", From: "swallow"},
	)
	spec.Stages = []domain.StageSpec{
		{Name: "forward", Type: forwardingType, From: "source"},
		{Name: "swallow", Type: "fake", From: "source"},
	}
	spec.Metrics.Records.Stages = new(true)
	_, err := c.UpsertStream(t.Context(), spec)
	require.NoError(t, err)

	require.NoError(t, deliver(t, reg, logs(2)))

	assert.Equal(t, map[attribute.Distinct]int64{
		attrs("source", "source", signal(domain.SignalLogs)): 2,
		attrs("forward", "stage", signal(domain.SignalLogs)): 2,
	}, collect(t, reader, recordsOutMetric), "a stage that passes nothing on has no output")
	assert.Equal(t, map[attribute.Distinct]int64{
		attrs("source", "source", signal(domain.SignalLogs)): 2,
		attrs("forward", "stage", signal(domain.SignalLogs)): 2,
		attrs("swallow", "stage", signal(domain.SignalLogs)): 2,
		attrs("a", "sink", signal(domain.SignalLogs)):        2,
		attrs("b", "sink", signal(domain.SignalLogs)):        2,
	}, collect(t, reader, recordsMetric))
}

func TestMetricsRecordCancellation(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		ctx     func(t *testing.T) context.Context
		outcome string
	}{
		{
			name: "caller cancels",
			ctx: func(t *testing.T) context.Context {
				ctx, cancel := context.WithCancel(t.Context())
				time.AfterFunc(10*time.Millisecond, cancel)
				return ctx
			},
			outcome: "cancelled",
		},
		{
			name: "deadline expires",
			ctx: func(t *testing.T) context.Context {
				ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
				t.Cleanup(cancel)
				return ctx
			},
			outcome: "error",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c, reg, reader := newMeteredCore(t)
			_, err := c.UpsertStream(t.Context(), streamSpec(1, sink("waiting", blockingType)))
			require.NoError(t, err)

			require.Error(t, reg.sources[0].subs[0].Notify(tc.ctx(t), logs(1)))

			assert.Equal(t, map[attribute.Distinct]int64{
				attrs("source", "source", outcome(tc.outcome)): 1,
				attrs("waiting", "sink", outcome(tc.outcome)):  1,
			}, collect(t, reader, durationMetric))
		})
	}
}

// requestSizes returns the count and sum of every point of the request size
// histogram, by attribute set.
func requestSizes(t *testing.T, reader *sdkmetric.ManualReader) map[attribute.Distinct][2]int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))

	points := map[attribute.Distinct][2]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if data, ok := m.Data.(metricdata.Histogram[int64]); ok && m.Name == "tram.source.request.size" {
				for _, dp := range data.DataPoints {
					points[dp.Attributes.Equivalent()] = [2]int64{int64(dp.Count), dp.Sum}
				}
			}
		}
	}
	return points
}

// Sources that decode requests report their size as received and decoded;
// only the source records it, however many nodes the event passes.
func TestMetricsRecordRequestSizeOfSources(t *testing.T) {
	t.Parallel()

	c, reg, reader := newMeteredCore(t)
	_, err := c.UpsertStream(t.Context(), streamSpec(1, sink("sink", "fake")))
	require.NoError(t, err)

	event := logs(2)
	event.Request = &domain.Request{Encoding: "gzip", WireBytes: 100, DecodedBytes: 500}
	require.NoError(t, deliver(t, reg, event))
	require.NoError(t, deliver(t, reg, event))
	require.NoError(t, deliver(t, reg, logs(2)), "events without a request record nothing")

	encoding := attribute.String("encoding", "gzip")
	assert.Equal(t, map[attribute.Distinct][2]int64{
		attrs("source", "source", encoding, attribute.String("body", "received")): {2, 200},
		attrs("source", "source", encoding, attribute.String("body", "decoded")):  {2, 1000},
	}, requestSizes(t, reader))
}
