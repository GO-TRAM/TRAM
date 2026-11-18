// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package broker_test

import (
	"fmt"
	"log/slog"
	"maps"
	"sort"
	"testing"

	"github.com/go-playground/validator/v10"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	contract "codeberg.org/eljoth/tram/internal/adapters/broker"
	"codeberg.org/eljoth/tram/internal/adapters/broker/brokertest"
	"codeberg.org/eljoth/tram/internal/adapters/broker/partition"
	"codeberg.org/eljoth/tram/internal/adapters/sink/broker"
	"codeberg.org/eljoth/tram/internal/core/domain"
	"codeberg.org/eljoth/tram/internal/testutil/otlpgen"
)

const byTeam = "'out.' + record.attributes.get_attr('team')"

func newSink(t *testing.T, m *brokertest.Memory, spec map[string]any) (*broker.Sink, error) {
	t.Helper()
	s, err := broker.NewWithDrivers(domain.SinkSpec{Name: "out", From: "in", Spec: spec},
		validator.New(validator.WithRequiredStructEnabled()), slog.New(slog.DiscardHandler),
		contract.Drivers[contract.PublisherDriver]{brokertest.MemoryName: m})
	if err == nil {
		t.Cleanup(func() { _ = s.Stop() })
	}
	return s, err
}

func memorySpec(extra map[string]any) map[string]any {
	spec := map[string]any{
		"driver":      brokertest.MemoryName,
		"memory":      map[string]any{},
		"destination": map[string]any{"logs": byTeam},
	}
	maps.Copy(spec, extra)
	return spec
}

func TestNewRejectsInvalidSpecs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		spec map[string]any
	}{
		{name: "no driver", spec: map[string]any{"destination": map[string]any{"logs": byTeam}}},
		{name: "unknown driver", spec: memorySpec(map[string]any{"driver": "kafka"})},
		{name: "options missing", spec: map[string]any{
			"driver": brokertest.MemoryName, "destination": map[string]any{"logs": byTeam},
		}},
		{name: "options of another driver", spec: memorySpec(map[string]any{"nats": map[string]any{}})},
		{name: "no destination", spec: memorySpec(map[string]any{"destination": map[string]any{}})},
		{name: "invalid CEL", spec: memorySpec(map[string]any{"destination": map[string]any{"logs": "'out' +"}})},
		{name: "negative max_pending", spec: memorySpec(map[string]any{"max_pending": float64(-1)})},
		{name: "partition without key of a sent signal", spec: memorySpec(map[string]any{
			"partition": map[string]any{"count": float64(4)},
		})},
		{name: "partition key of a signal not sent", spec: memorySpec(map[string]any{
			"partition": map[string]any{"count": float64(4), "logs": "'k'", "traces": "'k'"},
		})},
		{name: "partition count 0", spec: memorySpec(map[string]any{
			"partition": map[string]any{"count": float64(0), "logs": "'k'"},
		})},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := newSink(t, brokertest.NewMemory(0), tt.spec)
			assert.Error(t, err)
		})
	}
}

// bodies decodes the published messages as logs, by destination.
func bodies(t *testing.T, messages []contract.Message) map[string]int {
	t.Helper()
	out := map[string]int{}
	for _, m := range messages {
		event, err := domain.UnmarshalOTLPData(domain.SignalLogs, m.Body)
		require.NoError(t, err, "the body is plain OTLP")
		out[m.Destination] += event.RecordCount()
	}
	return out
}

func TestNotifyGroupsByDestination(t *testing.T) {
	t.Parallel()

	m := brokertest.NewMemory(0)
	s, err := newSink(t, m, memorySpec(nil))
	require.NoError(t, err)

	logs := otlpgen.Logs(otlpgen.Shape{Records: 10, Resources: 1, Teams: 2, Attributes: 1})
	require.NoError(t, s.Notify(t.Context(), &domain.Event{Logs: logs}))

	assert.Equal(t, map[string]int{"out.team-0": 5, "out.team-1": 5}, bodies(t, m.Published()))
}

func TestNotifySendsEachSignal(t *testing.T) {
	t.Parallel()

	m := brokertest.NewMemory(0)
	s, err := newSink(t, m, memorySpec(map[string]any{"destination": map[string]any{
		"logs": "'out.logs'", "metrics": "'out.metrics'", "traces": "'out.traces'",
	}}))
	require.NoError(t, err)

	shape := otlpgen.Shape{Records: 2, Resources: 1, Teams: 1}
	for _, event := range []*domain.Event{
		{Logs: otlpgen.Logs(shape)}, {Metrics: otlpgen.Metrics(shape)}, {Traces: otlpgen.Traces(shape)},
	} {
		require.NoError(t, s.Notify(t.Context(), event))
	}

	got := map[string]domain.Signal{}
	for _, msg := range m.Published() {
		for _, signal := range []domain.Signal{domain.SignalLogs, domain.SignalMetrics, domain.SignalTraces} {
			if fmt.Sprintf("out.%s", signal) == msg.Destination {
				event, decodeErr := domain.UnmarshalOTLPData(signal, msg.Body)
				require.NoError(t, decodeErr)
				assert.Equal(t, 2, event.RecordCount())
				got[msg.Destination] = signal
			}
		}
	}
	assert.Len(t, got, 3)
}

func TestNotifyKeepsAnUnsplitPayload(t *testing.T) {
	t.Parallel()

	m := brokertest.NewMemory(0)
	s, err := newSink(t, m, memorySpec(map[string]any{"destination": map[string]any{"logs": "'out.all'"}}))
	require.NoError(t, err)

	logs := otlpgen.Logs(otlpgen.Shape{Records: 4, Resources: 2, Teams: 2, Attributes: 1})
	require.NoError(t, s.Notify(t.Context(), &domain.Event{Logs: logs}))

	published := m.Published()
	require.Len(t, published, 1)
	want, err := proto.Marshal(logs)
	require.NoError(t, err)
	assert.Equal(t, want, published[0].Body, "records of one destination are sent as they came")
}

func TestNotifyPartitions(t *testing.T) {
	t.Parallel()

	m := brokertest.NewMemory(0)
	s, err := newSink(t, m, memorySpec(map[string]any{
		"destination": map[string]any{"logs": "'out.' + string(partition) + '.' + record.attributes.get_attr('team')"},
		"partition":   map[string]any{"count": float64(8), "logs": "record.attributes.get_attr('team')"},
	}))
	require.NoError(t, err)

	logs := otlpgen.Logs(otlpgen.Shape{Records: 6, Resources: 1, Teams: 3, Attributes: 1})
	require.NoError(t, s.Notify(t.Context(), &domain.Event{Logs: logs}))

	var got []string
	for dest := range bodies(t, m.Published()) {
		got = append(got, dest)
	}
	sort.Strings(got)
	var want []string
	for _, team := range []string{"team-0", "team-1", "team-2"} {
		want = append(want, fmt.Sprintf("out.%d.%s", partition.Of(team, 8), team))
	}
	sort.Strings(want)
	assert.Equal(t, want, got, "each team in the partition of its key")
}

func TestNotifyRejectsOrFails(t *testing.T) {
	t.Parallel()

	logs := otlpgen.Logs(otlpgen.Shape{Records: 4, Resources: 1, Teams: 1, Attributes: 1})
	tests := []struct {
		name         string
		spec         map[string]any
		maxSize      int
		fail         string
		wantRejected bool
	}{
		{name: "destination with a wildcard", wantRejected: true,
			spec: memorySpec(map[string]any{"destination": map[string]any{"logs": "'out.*'"}})},
		{name: "destination expression not a string", wantRejected: true,
			spec: memorySpec(map[string]any{"destination": map[string]any{"logs": "1"}})},
		{name: "message above the broker's maximum", wantRejected: true, maxSize: 10, spec: memorySpec(nil)},
		{name: "broker refuses the publish", fail: "out.team-0", spec: memorySpec(nil)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			m := brokertest.NewMemory(tt.maxSize)
			if tt.fail != "" {
				m.Fail(tt.fail, true)
			}
			s, err := newSink(t, m, tt.spec)
			require.NoError(t, err)

			err = s.Notify(t.Context(), &domain.Event{Logs: logs})
			require.Error(t, err)
			if tt.wantRejected {
				require.ErrorIs(t, err, domain.ErrEventRejected, "the same event would fail again")
			} else {
				require.NotErrorIs(t, err, domain.ErrEventRejected, "the source may retry")
			}
			assert.Empty(t, m.Published())
		})
	}
}
