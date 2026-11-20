// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package domain_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"

	"github.com/go-tram/tram/internal/core/domain"
	"github.com/go-tram/tram/internal/testutil/otlpgen"
)

const (
	team0           = "team-0"
	inputUnmodified = "the input must not be modified"
)

func team(attrs []*commonpb.KeyValue) string {
	for _, a := range attrs {
		if a.GetKey() == "team" {
			return a.GetValue().GetStringValue()
		}
	}
	return ""
}

func logTeams(data *logspb.LogsData) []string {
	var teams []string
	for _, rl := range data.GetResourceLogs() {
		for _, sl := range rl.GetScopeLogs() {
			for _, r := range sl.GetLogRecords() {
				teams = append(teams, team(r.GetAttributes()))
			}
		}
	}
	return teams
}

func keepTeam(name string) domain.LogFilter {
	return func(_ *resourcepb.Resource, _ *commonpb.InstrumentationScope, r *logspb.LogRecord) (bool, error) {
		return team(r.GetAttributes()) == name, nil
	}
}

func TestFilterLogs(t *testing.T) {
	t.Parallel()

	shape := otlpgen.Shape{Records: 8, Resources: 2, Teams: 2}

	cases := []struct {
		name     string
		keep     domain.LogFilter
		expected []string
	}{
		{name: "keep one team", keep: keepTeam(team0), expected: []string{team0, team0, team0, team0}},
		{name: "keep none", keep: keepTeam("team-9")},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			input := otlpgen.Logs(shape)
			original := proto.Clone(input)

			got, err := domain.FilterLogs(input, c.keep)
			require.NoError(t, err)

			assert.Equal(t, c.expected, logTeams(got))
			assert.True(t, proto.Equal(original, input), inputUnmodified)
			if c.expected == nil {
				assert.Nil(t, got)
			}
		})
	}
}

func TestFilterLogsKeepAllReturnsInput(t *testing.T) {
	t.Parallel()

	input := otlpgen.Logs(otlpgen.Shape{Records: 8, Resources: 2, Teams: 2})
	got, err := domain.FilterLogs(
		input,
		func(*resourcepb.Resource, *commonpb.InstrumentationScope, *logspb.LogRecord) (bool, error) {
			return true, nil
		},
	)
	require.NoError(t, err)
	assert.Same(t, input, got)
}

func TestFilterLogsSharesUnfilteredParts(t *testing.T) {
	t.Parallel()

	input := otlpgen.Logs(otlpgen.Shape{Records: 8, Resources: 2, Teams: 1})
	dropped := input.GetResourceLogs()[1].GetScopeLogs()[0].GetLogRecords()[0]

	got, err := domain.FilterLogs(
		input,
		func(_ *resourcepb.Resource, _ *commonpb.InstrumentationScope, r *logspb.LogRecord) (bool, error) {
			return r != dropped, nil
		},
	)
	require.NoError(t, err)

	require.Len(t, got.GetResourceLogs(), 2)
	assert.Same(t, input.GetResourceLogs()[0], got.GetResourceLogs()[0], "a fully kept resource is reused")
	assert.NotSame(t, input.GetResourceLogs()[1], got.GetResourceLogs()[1])
	assert.Same(t, input.GetResourceLogs()[1].GetResource(), got.GetResourceLogs()[1].GetResource())
	assert.Same(
		t,
		input.GetResourceLogs()[1].GetScopeLogs()[0].GetLogRecords()[1],
		got.GetResourceLogs()[1].GetScopeLogs()[0].GetLogRecords()[0],
	)
	assert.Len(t, got.GetResourceLogs()[1].GetScopeLogs()[0].GetLogRecords(), 3)
}

func TestFilterLogsRemovesEmptyContainers(t *testing.T) {
	t.Parallel()

	keep := &logspb.LogRecord{SeverityText: "keep"}
	input := &logspb.LogsData{ResourceLogs: []*logspb.ResourceLogs{
		{ScopeLogs: []*logspb.ScopeLogs{
			{LogRecords: []*logspb.LogRecord{{SeverityText: "drop"}}},
			{LogRecords: []*logspb.LogRecord{keep}},
		}},
		{ScopeLogs: []*logspb.ScopeLogs{{LogRecords: []*logspb.LogRecord{{SeverityText: "drop"}}}}},
	}}

	got, err := domain.FilterLogs(
		input,
		func(_ *resourcepb.Resource, _ *commonpb.InstrumentationScope, r *logspb.LogRecord) (bool, error) {
			return r.GetSeverityText() == "keep", nil
		},
	)
	require.NoError(t, err)

	require.Len(t, got.GetResourceLogs(), 1)
	require.Len(t, got.GetResourceLogs()[0].GetScopeLogs(), 1)
	assert.Same(t, input.GetResourceLogs()[0].GetScopeLogs()[1], got.GetResourceLogs()[0].GetScopeLogs()[0])
	assert.Equal(t, []*logspb.LogRecord{keep}, got.GetResourceLogs()[0].GetScopeLogs()[0].GetLogRecords())
}

func TestFilterErrorsPropagate(t *testing.T) {
	t.Parallel()

	boom := errors.New("boom")
	shape := otlpgen.Shape{Records: 4, Resources: 1, Teams: 1}

	_, err := domain.FilterLogs(
		otlpgen.Logs(shape),
		func(*resourcepb.Resource, *commonpb.InstrumentationScope, *logspb.LogRecord) (bool, error) {
			return false, boom
		},
	)
	require.ErrorIs(t, err, boom)

	_, err = domain.FilterMetrics(
		otlpgen.Metrics(shape),
		func(*resourcepb.Resource, *commonpb.InstrumentationScope, *metricspb.Metric, proto.Message) (bool, error) {
			return false, boom
		},
	)
	require.ErrorIs(t, err, boom)

	_, err = domain.FilterSpans(
		otlpgen.Traces(shape),
		func(*resourcepb.Resource, *commonpb.InstrumentationScope, *tracepb.Span) (bool, error) {
			return false, boom
		},
	)
	assert.ErrorIs(t, err, boom)
}

func TestFilterNil(t *testing.T) {
	t.Parallel()

	logs, err := domain.FilterLogs(nil, keepTeam(team0))
	require.NoError(t, err)
	assert.Nil(t, logs)
}

func TestFilterSpans(t *testing.T) {
	t.Parallel()

	input := otlpgen.Traces(otlpgen.Shape{Records: 8, Resources: 2, Teams: 2})
	original := proto.Clone(input)

	got, err := domain.FilterSpans(
		input,
		func(_ *resourcepb.Resource, _ *commonpb.InstrumentationScope, s *tracepb.Span) (bool, error) {
			return team(s.GetAttributes()) == "team-1", nil
		},
	)
	require.NoError(t, err)

	assert.Equal(t, 4, (&domain.Event{Traces: got}).RecordCount())
	for _, rs := range got.GetResourceSpans() {
		for _, ss := range rs.GetScopeSpans() {
			for _, s := range ss.GetSpans() {
				assert.Equal(t, "team-1", team(s.GetAttributes()))
			}
		}
	}
	assert.True(t, proto.Equal(original, input), inputUnmodified)
}

func TestFilterMetricsPerDataPoint(t *testing.T) {
	t.Parallel()

	attrs := func(team string) []*commonpb.KeyValue {
		return []*commonpb.KeyValue{
			{Key: "team", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: team}}},
		}
	}
	sum := &metricspb.Metric{Name: "requests", Unit: "1", Data: &metricspb.Metric_Sum{Sum: &metricspb.Sum{
		AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
		IsMonotonic:            true,
		DataPoints:             []*metricspb.NumberDataPoint{{Attributes: attrs("a")}, {Attributes: attrs("b")}},
	}}}
	histogram := &metricspb.Metric{Name: "latency", Data: &metricspb.Metric_Histogram{Histogram: &metricspb.Histogram{
		AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA,
		DataPoints:             []*metricspb.HistogramDataPoint{{Attributes: attrs("b")}, {Attributes: attrs("b")}},
	}}}
	summary := &metricspb.Metric{Name: "sizes", Data: &metricspb.Metric_Summary{Summary: &metricspb.Summary{
		DataPoints: []*metricspb.SummaryDataPoint{{Attributes: attrs("a")}},
	}}}
	noData := &metricspb.Metric{Name: "empty"}
	input := &metricspb.MetricsData{ResourceMetrics: []*metricspb.ResourceMetrics{{
		ScopeMetrics: []*metricspb.ScopeMetrics{{Metrics: []*metricspb.Metric{sum, histogram, summary, noData}}},
	}}}
	original := proto.Clone(input)

	var seen []string
	got, err := domain.FilterMetrics(
		input,
		func(_ *resourcepb.Resource, _ *commonpb.InstrumentationScope, m *metricspb.Metric, dp proto.Message) (bool, error) {
			seen = append(seen, m.GetName())
			dpAttrs := dp.ProtoReflect().Get(dp.ProtoReflect().Descriptor().Fields().ByName("attributes")).List()
			return dpAttrs.Get(0).Message().Interface().(*commonpb.KeyValue).GetValue().GetStringValue() == "a", nil
		},
	)
	require.NoError(t, err)

	assert.Equal(t, []string{"requests", "requests", "latency", "latency", "sizes"}, seen)
	metrics := got.GetResourceMetrics()[0].GetScopeMetrics()[0].GetMetrics()
	require.Len(t, metrics, 3, "the histogram lost all data points, the metric without data is kept")

	gotSum := metrics[0]
	assert.Equal(t, "requests", gotSum.GetName())
	assert.Equal(t, "1", gotSum.GetUnit())
	assert.Len(t, gotSum.GetSum().GetDataPoints(), 1)
	assert.Equal(
		t,
		metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
		gotSum.GetSum().GetAggregationTemporality(),
	)
	assert.True(t, gotSum.GetSum().GetIsMonotonic())

	assert.Same(t, summary, metrics[1], "a fully kept metric is reused")
	assert.Same(t, noData, metrics[2])
	assert.True(t, proto.Equal(original, input), inputUnmodified)
}
