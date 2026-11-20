// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package stage_test

import (
	"context"
	"log/slog"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"

	"github.com/go-tram/tram/internal/adapters/stage"
	"github.com/go-tram/tram/internal/adapters/stage/batch"
	"github.com/go-tram/tram/internal/adapters/stage/delay"
	"github.com/go-tram/tram/internal/adapters/stage/filter"
	"github.com/go-tram/tram/internal/adapters/stage/noop"
	"github.com/go-tram/tram/internal/adapters/stage/validation"
	"github.com/go-tram/tram/internal/adapters/stage/valuemap"
	"github.com/go-tram/tram/internal/core/domain"
	"github.com/go-tram/tram/internal/testutil/otlpgen"
)

type collector struct {
	mu     sync.Mutex
	events []*domain.Event
}

func (c *collector) Name() string { return "collector" }
func (c *collector) From() string { return "" }
func (c *collector) Stop() error  { return nil }

func (c *collector) Notify(_ context.Context, event *domain.Event) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.events = append(c.events, event)
	return nil
}

func (c *collector) records() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, e := range c.events {
		n += e.RecordCount()
	}
	return n
}

func (c *collector) logRecords() []*logspb.LogRecord {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []*logspb.LogRecord
	for _, e := range c.events {
		for _, rl := range e.Logs.GetResourceLogs() {
			for _, sl := range rl.GetScopeLogs() {
				out = append(out, sl.GetLogRecords()...)
			}
		}
	}
	return out
}

func hasAttr(record *logspb.LogRecord, key, value string) bool {
	for _, a := range record.GetAttributes() {
		if a.GetKey() == key && (value == "" || a.GetValue().GetStringValue() == value) {
			return true
		}
	}
	return false
}

// An event is shared by reference between the branches of a stream and
// delivered to them concurrently (see domain.Event). Every stage must leave
// its input as it is, including when the same event reaches it from several
// goroutines while other branches read it. Run with -race.
func TestStagesLeaveTheSharedEventUnchanged(t *testing.T) {
	t.Parallel()

	const senders, perSender = 8, 10
	shape := otlpgen.Shape{Records: 4, Resources: 2, Teams: 2, Attributes: 2}
	newEvent := func() *domain.Event {
		return &domain.Event{
			Logs:    otlpgen.Logs(shape),
			Metrics: otlpgen.Metrics(shape),
			Traces:  otlpgen.Traces(shape),
		}
	}
	sent := senders * perSender * newEvent().RecordCount()

	cases := []struct {
		name  string
		spec  domain.StageSpec
		check func(t *testing.T, out *collector)
	}{
		{
			name:  noop.Type,
			spec:  domain.StageSpec{Type: noop.Type},
			check: func(t *testing.T, out *collector) { assert.Equal(t, sent, out.records()) },
		},
		{
			name: validation.Type,
			spec: domain.StageSpec{Type: validation.Type, Spec: map[string]any{
				"driver":      "cel",
				"on_mismatch": "reject",
				"cel": map[string]any{
					"logs": "logs.resource_logs.all(r, r.scope_logs.all(s, s.log_records.all(l, l.attributes.exists(a, a.key == 'team'))))",
				},
			}},
			check: func(t *testing.T, out *collector) { assert.Equal(t, sent, out.records()) },
		},
		{
			name: filter.Type,
			spec: domain.StageSpec{Type: filter.Type, Spec: map[string]any{
				"driver": "cel",
				"cel": map[string]any{
					"logs": "record.attributes.exists(a, a.key == 'team' && a.value.string_value == 'team-0')",
				},
			}},
			check: func(t *testing.T, out *collector) {
				records := out.logRecords()
				require.NotEmpty(t, records)
				for _, r := range records {
					require.True(t, hasAttr(r, "team", "team-0"), "only team-0 passes the filter")
				}
			},
		},
		{
			name:  delay.Type,
			spec:  domain.StageSpec{Type: delay.Type, Spec: map[string]any{"delay": "1ms"}},
			check: func(t *testing.T, out *collector) { assert.Equal(t, sent, out.records()) },
		},
		{
			name: batch.Type,
			spec: domain.StageSpec{Type: batch.Type, Spec: map[string]any{
				"max_records": float64(5 * newEvent().RecordCount()), "timeout": "10ms",
			}},
			check: func(t *testing.T, out *collector) { assert.Equal(t, sent, out.records()) },
		},
		{
			name: valuemap.Type,
			spec: domain.StageSpec{Type: valuemap.Type, Spec: map[string]any{
				"source_attr": "team", "target_attr": "owner", "default": "nobody",
				"table": map[string]any{"team-0": "alpha"},
			}},
			check: func(t *testing.T, out *collector) {
				records := out.logRecords()
				require.NotEmpty(t, records)
				for _, r := range records {
					require.True(t, hasAttr(r, "owner", ""), "forwarded records are annotated")
				}
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			c.spec.Name, c.spec.From = c.name, "in"
			s, err := stage.NewFactory(slog.New(slog.DiscardHandler)).New(c.spec)
			require.NoError(t, err)
			out := &collector{}
			require.NoError(t, s.Subscribe(out))

			event := newEvent()
			logs := proto.Clone(event.Logs).(*logspb.LogsData)
			metrics := proto.Clone(event.Metrics).(*metricspb.MetricsData)
			traces := proto.Clone(event.Traces).(*tracepb.TracesData)

			notifyAndRead(t, s, event, senders, perSender)
			require.NoError(t, s.Stop(), "stopping flushes what the stage holds")

			assert.True(t, proto.Equal(logs, event.Logs), "the logs of the input are unchanged")
			assert.True(t, proto.Equal(metrics, event.Metrics), "the metrics of the input are unchanged")
			assert.True(t, proto.Equal(traces, event.Traces), "the traces of the input are unchanged")
			c.check(t, out)
		})
	}
}

// notifyAndRead delivers event to s from several goroutines at once, while
// others read it like sibling branches would.
func notifyAndRead(t *testing.T, s domain.Stage, event *domain.Event, senders, perSender int) {
	t.Helper()
	var wg sync.WaitGroup
	done := make(chan struct{})
	for range senders {
		wg.Go(func() {
			for range perSender {
				assert.NoError(t, s.Notify(t.Context(), event))
			}
		})
	}
	var readers sync.WaitGroup
	for range 2 {
		readers.Go(func() {
			for {
				select {
				case <-done:
					return
				default:
				}
				_ = event.RecordCount()
				_, err := event.MarshalOTLPPayloads()
				assert.NoError(t, err)
			}
		})
	}
	wg.Wait()
	close(done)
	readers.Wait()
}
