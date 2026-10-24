// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package nats_test

import (
	"fmt"
	"testing"

	"github.com/go-playground/validator/v10"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"

	"codeberg.org/eljoth/tram/internal/adapters/natspartition"
	natssink "codeberg.org/eljoth/tram/internal/adapters/sink/nats"
	"codeberg.org/eljoth/tram/internal/core/domain"
)

const (
	tenantKey     = "record.attributes.get_attr('tenant')"
	tenantSubject = "'audit.' + string(partition) + '.' + record.attributes.get_attr('tenant')"
)

func partitionedSpec(count int, subject map[string]any, partition map[string]any) map[string]any {
	if _, ok := subject["register"]; !ok {
		subject["register"] = []any{"audit.{partition}.>"}
	}
	partition["count"] = count
	return map[string]any{
		"url":       "nats://localhost:4222",
		"stream":    "audit-{partition}",
		"subject":   subject,
		"partition": partition,
	}
}

func partitionedSink(t *testing.T, count int, subject, key string) *natssink.NATS {
	t.Helper()
	sink, err := natssink.NewWithoutConnection(
		partitionedSpec(count, map[string]any{"logs": subject}, map[string]any{"logs": key}),
		validator.New())
	require.NoError(t, err)
	return sink
}

func tenantLogs(tenants ...string) *domain.Event {
	records := make([]*logspb.LogRecord, len(tenants))
	for i, tenant := range tenants {
		records[i] = &logspb.LogRecord{Attributes: []*commonpb.KeyValue{stringValue("tenant", tenant)}}
	}
	return &domain.Event{Logs: &logspb.LogsData{ResourceLogs: []*logspb.ResourceLogs{{
		ScopeLogs: []*logspb.ScopeLogs{{LogRecords: records}},
	}}}}
}

func stringValue(key, value string) *commonpb.KeyValue {
	return &commonpb.KeyValue{
		Key:   key,
		Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: value}},
	}
}

func TestPartitionSpec(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		spec map[string]any
		err  string
	}{
		{
			name: "stream without placeholder",
			spec: func() map[string]any {
				s := partitionedSpec(4, map[string]any{"logs": tenantSubject}, map[string]any{"logs": tenantKey})
				s["stream"] = "audit"
				return s
			}(),
			err: "stream audit needs {partition}",
		},
		{
			name: "register subject without placeholder",
			spec: partitionedSpec(4,
				map[string]any{"register": []any{"audit.{partition}.>", "other.>"}, "logs": tenantSubject},
				map[string]any{"logs": tenantKey}),
			err: "register subject other.> needs {partition}",
		},
		{
			name: "published signal without key",
			spec: partitionedSpec(4, map[string]any{"logs": tenantSubject, "traces": "'audit.0.t'"},
				map[string]any{"logs": tenantKey}),
			err: "traces are published, but partition.traces is not set",
		},
		{
			name: "key of a signal that is not published",
			spec: partitionedSpec(4, map[string]any{"logs": tenantSubject},
				map[string]any{"logs": tenantKey, "metrics": "metric.name"}),
			err: "partition.metrics is set, but metrics are not published",
		},
		{
			name: "invalid key",
			spec: partitionedSpec(4, map[string]any{"logs": tenantSubject}, map[string]any{"logs": "unknown.field"}),
			err:  "key:",
		},
		{
			name: "key cannot read the partition",
			spec: partitionedSpec(
				4,
				map[string]any{"logs": tenantSubject},
				map[string]any{"logs": "string(partition)"},
			),
			err: "key:",
		},
		{
			name: "count 0",
			spec: partitionedSpec(0, map[string]any{"logs": tenantSubject}, map[string]any{"logs": tenantKey}),
		},
		{
			name: "count above 1024",
			spec: partitionedSpec(1025, map[string]any{"logs": tenantSubject}, map[string]any{"logs": tenantKey}),
		},
		{
			name: "placeholder without partitioning",
			spec: map[string]any{
				"url": "nats://localhost:4222", "stream": "audit-{partition}",
				"subject": map[string]any{"register": []any{"audit.>"}, "logs": "'audit.x'"},
			},
			err: "stream audit-{partition} has {partition}, but partition is not set",
		},
		{
			name: "partition variable without partitioning",
			spec: map[string]any{
				"url": "nats://localhost:4222", "stream": "audit",
				"subject": map[string]any{"register": []any{"audit.>"}, "logs": "'audit.' + string(partition)"},
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			_, err := natssink.NewWithoutConnection(c.spec, validator.New())
			require.Error(t, err)
			if c.err != "" {
				assert.ErrorContains(t, err, c.err)
			}
		})
	}
}

func TestTargets(t *testing.T) {
	t.Parallel()

	spec := &natssink.Spec{
		Stream:    "audit-{partition}",
		Subject:   natssink.SubjectSpec{Register: []string{"audit.{partition}.>", "audit.{partition}"}},
		Partition: &natssink.PartitionSpec{Count: 3},
	}
	assert.Equal(t, []natssink.Target{
		{Name: "audit-0", Subjects: []string{"audit.0.>", "audit.0"}},
		{Name: "audit-1", Subjects: []string{"audit.1.>", "audit.1"}},
		{Name: "audit-2", Subjects: []string{"audit.2.>", "audit.2"}},
	}, natssink.Targets(spec))

	spec.Partition = nil
	spec.Stream = "audit"
	spec.Subject.Register = []string{"audit.>"}
	assert.Equal(t, []natssink.Target{{Name: "audit", Subjects: []string{"audit.>"}}}, natssink.Targets(spec))
}

func TestGroupEventsByPartition(t *testing.T) {
	t.Parallel()

	sink := partitionedSink(t, 4, tenantSubject, tenantKey)
	tenants := []string{"t1", "t2", "t3", "t4", "t5", "t6", "t1"}
	messages, err := natssink.GroupEventsBySubjects(sink, tenantLogs(tenants...))
	require.NoError(t, err)

	subjects := map[string]int{}
	for _, m := range messages {
		subjects[m.Subject] += len(m.Event.Logs.GetResourceLogs()[0].GetScopeLogs()[0].GetLogRecords())
	}
	expected := map[string]int{}
	for _, tenant := range tenants {
		expected[fmt.Sprintf("audit.%d.%s", natspartition.Of(tenant, 4), tenant)]++
	}
	assert.Equal(t, expected, subjects)
}

func TestGroupEventsByPartitionPerDataPoint(t *testing.T) {
	t.Parallel()

	sink, err := natssink.NewWithoutConnection(partitionedSpec(8,
		map[string]any{"metrics": "'audit.' + string(partition) + '.' + datapoint.attributes.get_attr('tenant')"},
		map[string]any{"metrics": "datapoint.attributes.get_attr('tenant')"},
	), validator.New())
	require.NoError(t, err)

	event := &domain.Event{Metrics: &metricspb.MetricsData{ResourceMetrics: []*metricspb.ResourceMetrics{{
		ScopeMetrics: []*metricspb.ScopeMetrics{{Metrics: []*metricspb.Metric{{
			Name: "requests",
			Data: &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{DataPoints: []*metricspb.NumberDataPoint{
				{Attributes: []*commonpb.KeyValue{stringValue("tenant", "a")}},
				{Attributes: []*commonpb.KeyValue{stringValue("tenant", "b")}},
			}}},
		}}}},
	}}}}

	messages, err := natssink.GroupEventsBySubjects(sink, event)
	require.NoError(t, err)
	var subjects []string
	for _, m := range messages {
		subjects = append(subjects, m.Subject)
	}
	assert.ElementsMatch(t, []string{
		fmt.Sprintf("audit.%d.a", natspartition.Of("a", 8)),
		fmt.Sprintf("audit.%d.b", natspartition.Of("b", 8)),
	}, subjects)
}

func TestNotifyRejectsSubjectOutsideItsPartition(t *testing.T) {
	t.Parallel()

	// A tenant in another partition than 0 would end up in the stream of
	// partition 0.
	tenant := "t1"
	require.NotEqual(t, 0, natspartition.Of(tenant, 4))

	sink := partitionedSink(t, 4, "'audit.0.' + record.attributes.get_attr('tenant')", tenantKey)
	err := sink.Notify(t.Context(), tenantLogs(tenant))
	require.ErrorIs(t, err, domain.ErrEventRejected)
	assert.ErrorContains(t, err, fmt.Sprintf("is not stored in partition %d", natspartition.Of(tenant, 4)))
}

func TestNotifyRejectsFailingKey(t *testing.T) {
	t.Parallel()

	sink := partitionedSink(t, 4, tenantSubject, "record.attributes[3].key")
	require.ErrorIs(t, sink.Notify(t.Context(), tenantLogs("t1")), domain.ErrEventRejected)
}

func TestMergePartitionStreamConfig(t *testing.T) {
	t.Parallel()

	spec := &natssink.Spec{
		Stream:    "audit-{partition}",
		Subject:   natssink.SubjectSpec{Register: []string{"audit.{partition}.>"}},
		Partition: &natssink.PartitionSpec{Count: 4},
	}
	existing := func(metadata map[string]string) jetstream.StreamConfig {
		return jetstream.StreamConfig{Name: "audit-2", Subjects: []string{"audit.2.>"}, Metadata: metadata}
	}

	t.Run("adds the partition to a stream without it", func(t *testing.T) {
		t.Parallel()

		in := existing(map[string]string{"owner": "team"})
		merged, update, err := natssink.MergePartitionStreamConfig(in, spec, 2)
		require.NoError(t, err)
		assert.True(t, update.Changed)
		assert.Equal(t, map[string]string{
			"owner":                "team",
			"tram.partition.count": "4",
			"tram.partition.index": "2",
		}, merged.Metadata)
		assert.Equal(t, map[string]string{"owner": "team"}, in.Metadata, "the existing config is not modified")
	})

	t.Run("matching partition changes nothing", func(t *testing.T) {
		t.Parallel()

		_, update, err := natssink.MergePartitionStreamConfig(existing(natspartition.Metadata(2, 4)), spec, 2)
		require.NoError(t, err)
		assert.False(t, update.Changed)
	})

	t.Run("stream of another count fails", func(t *testing.T) {
		t.Parallel()

		_, _, err := natssink.MergePartitionStreamConfig(existing(natspartition.Metadata(2, 8)), spec, 2)
		require.ErrorIs(t, err, natspartition.ErrMismatch)
	})
}
