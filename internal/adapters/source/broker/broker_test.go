// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package broker_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"sync"
	"testing"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	contract "codeberg.org/eljoth/tram/internal/adapters/broker"
	"codeberg.org/eljoth/tram/internal/adapters/broker/brokertest"
	"codeberg.org/eljoth/tram/internal/adapters/broker/partition"
	"codeberg.org/eljoth/tram/internal/adapters/source/broker"
	"codeberg.org/eljoth/tram/internal/core/domain"
	"codeberg.org/eljoth/tram/internal/testutil/otlpgen"
)

const (
	waitFor = 5 * time.Second
	tick    = 10 * time.Millisecond
)

type collector struct {
	mu     sync.Mutex
	events []*domain.Event
	err    error
	hold   chan struct{}
}

func (c *collector) Name() string { return "collector" }
func (c *collector) From() string { return "in" }
func (c *collector) Stop() error  { return nil }
func (c *collector) Notify(_ context.Context, event *domain.Event) error {
	if c.hold != nil {
		<-c.hold
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil {
		return c.err
	}
	c.events = append(c.events, event)
	return nil
}

func (c *collector) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.events)
}

func spec(extra map[string]any) map[string]any {
	s := map[string]any{
		"driver": brokertest.MemoryName,
		"signal": "logs",
		"memory": map[string]any{"destination": "in.logs", "durable": "d"},
	}
	maps.Copy(s, extra)
	return s
}

func newSource(m *brokertest.Memory, s map[string]any) (*broker.Source, error) {
	return broker.New(domain.SourceSpec{Name: "in", Spec: s},
		validator.New(validator.WithRequiredStructEnabled()), slog.New(slog.DiscardHandler),
		contract.Drivers{brokertest.MemoryName: m})
}

// run starts the source and stops it when the test ends.
func run(t *testing.T, src *broker.Source, c *collector) {
	t.Helper()
	src.SetSubscribers([]domain.Subscriber{c})
	done := make(chan error, 1)
	go func() { done <- src.Run(context.Background()) }()
	t.Cleanup(func() {
		require.NoError(t, src.Stop())
		select {
		case err := <-done:
			assert.NoError(t, err)
		case <-time.After(waitFor):
			t.Error("source did not stop")
		}
	})
}

func logsBody(t *testing.T, records int) []byte {
	t.Helper()
	body, err := proto.Marshal(otlpgen.Logs(otlpgen.Shape{Records: records, Resources: 1, Teams: 1, Attributes: 1}))
	require.NoError(t, err)
	return body
}

func TestNewRejectsInvalidSpecs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		spec map[string]any
	}{
		{name: "no signal", spec: spec(map[string]any{"signal": ""})},
		{name: "unknown signal", spec: spec(map[string]any{"signal": "profiles"})},
		{name: "unknown driver", spec: spec(map[string]any{"driver": "kafka"})},
		{name: "options of another driver", spec: spec(map[string]any{"nats": map[string]any{}})},
		{name: "too many workers", spec: spec(map[string]any{"workers": float64(10001)})},
		{name: "placeholder without partition", spec: spec(map[string]any{
			"memory": map[string]any{"destination": "in.{partition}"},
		})},
		{name: "partition without placeholder", spec: spec(map[string]any{
			"partition": map[string]any{"count": float64(4), "key": "k"},
		})},
		{name: "partition without key", spec: spec(map[string]any{
			"memory":    map[string]any{"destination": "in.{partition}"},
			"partition": map[string]any{"count": float64(4)},
		})},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := newSource(brokertest.NewMemory(0), tt.spec)
			assert.Error(t, err)
		})
	}
}

func TestDeliveryOutcomes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body func(t *testing.T) []byte
		err  error
		want brokertest.Outcome
	}{
		{name: "delivered", body: func(t *testing.T) []byte { return logsBody(t, 3) },
			want: brokertest.Outcome{Acked: 1}},
		{name: "not valid OTLP", body: func(*testing.T) []byte { return []byte{0xff, 0xff, 0xff} },
			want: brokertest.Outcome{Rejected: 1}},
		{name: "rejected by the pipeline", body: func(t *testing.T) []byte { return logsBody(t, 1) },
			err: fmt.Errorf("%w: no", domain.ErrEventRejected), want: brokertest.Outcome{Rejected: 1}},
		{name: "pipeline failed for now", body: func(t *testing.T) []byte { return logsBody(t, 1) },
			err: errors.New("sink unavailable"), want: brokertest.Outcome{Retried: 1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			m := brokertest.NewMemory(0)
			m.Declare("in.logs", "d")
			m.Inject("in.logs", tt.body(t))
			src, err := newSource(m, spec(nil))
			require.NoError(t, err)
			c := &collector{err: tt.err}
			run(t, src, c)

			require.Eventually(t, func() bool { return m.Outcome("in.logs", "d") == tt.want }, waitFor, tick)
			if tt.want.Acked == 1 {
				require.Equal(t, 1, c.count())
				assert.Equal(t, 3, c.events[0].RecordCount())
			}
		})
	}
}

func TestPartitionSelectsTheKeysDestination(t *testing.T) {
	t.Parallel()

	m := brokertest.NewMemory(0)
	dest := fmt.Sprintf("in.%d", partition.Of("tenant-123", 8))
	m.Declare(dest, "d")
	m.Inject(dest, logsBody(t, 2))

	src, err := newSource(m, spec(map[string]any{
		"memory":    map[string]any{"destination": "in.{partition}", "durable": "d"},
		"partition": map[string]any{"count": float64(8), "key": "tenant-123"},
	}))
	require.NoError(t, err)
	c := &collector{}
	run(t, src, c)

	require.Eventually(t, func() bool { return c.count() == 1 }, waitFor, tick)
	assert.Equal(t, brokertest.Outcome{Acked: 1}, m.Outcome(dest, "d"))
}

func TestStopBeforeRun(t *testing.T) {
	t.Parallel()

	src, err := newSource(brokertest.NewMemory(0), spec(nil))
	require.NoError(t, err)
	require.NoError(t, src.Stop())
	assert.NoError(t, src.Run(t.Context()), "a stopped source does not start")
}

func TestStopWaitsForMessagesBeingHandled(t *testing.T) {
	t.Parallel()

	m := brokertest.NewMemory(0)
	m.Declare("in.logs", "d")
	m.Inject("in.logs", logsBody(t, 1))
	src, err := newSource(m, spec(nil))
	require.NoError(t, err)
	c := &collector{hold: make(chan struct{})}
	src.SetSubscribers([]domain.Subscriber{c})

	done := make(chan error, 1)
	go func() { done <- src.Run(context.Background()) }()
	require.Eventually(t, func() bool { return m.Outcome("in.logs", "d").Open == 1 }, waitFor, tick)

	require.NoError(t, src.Stop())
	select {
	case <-done:
		t.Fatal("Run returned while a message was being handled")
	case <-time.After(100 * time.Millisecond):
	}
	close(c.hold)
	require.NoError(t, <-done)
	assert.Equal(t, brokertest.Outcome{Acked: 1}, m.Outcome("in.logs", "d"), "the message is still acknowledged")
}
