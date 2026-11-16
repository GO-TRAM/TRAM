// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package cloudevent_test

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cesink "codeberg.org/eljoth/tram/internal/adapters/sink/cloudevent"
	cesource "codeberg.org/eljoth/tram/internal/adapters/source/cloudevent"
	"codeberg.org/eljoth/tram/internal/core/domain"
	"codeberg.org/eljoth/tram/internal/testutil/natstest"
	"codeberg.org/eljoth/tram/internal/testutil/otlpgen"
)

type collector struct {
	mu     sync.Mutex
	events []*domain.Event
	err    error
}

func (c *collector) Name() string { return "collector" }
func (c *collector) From() string { return "ce" }
func (c *collector) Stop() error  { return nil }
func (c *collector) Notify(_ context.Context, event *domain.Event) error {
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

func newValidator() *validator.Validate {
	return validator.New(validator.WithRequiredStructEnabled())
}

// createStream sets up the broker the way it is done outside of Tram.
func createStream(t *testing.T, url, name string) jetstream.Stream {
	t.Helper()
	nc, err := nats.Connect(url)
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	require.NoError(t, err)
	stream, err := js.CreateStream(t.Context(), jetstream.StreamConfig{Name: name, Subjects: []string{name + ".>"}})
	require.NoError(t, err)
	return stream
}

func newSource(t *testing.T, url, subject string, c *collector) *cesource.CloudEvent {
	t.Helper()
	source, err := cesource.New(domain.SourceSpec{Name: "ce", Spec: map[string]any{
		"broker":  "nats",
		"workers": float64(4),
		"nats":    map[string]any{"url": url, "subject": subject, "durable": "test"},
	}}, newValidator(), slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	source.SetSubscribers([]domain.Subscriber{c})
	return source
}

func run(t *testing.T, source *cesource.CloudEvent) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- source.Run(context.Background()) }()
	t.Cleanup(func() {
		require.NoError(t, source.Stop())
		select {
		case err := <-done:
			assert.NoError(t, err)
		case <-time.After(10 * time.Second):
			t.Error("source did not stop")
		}
	})
}

func TestRoundTrip(t *testing.T) {
	t.Parallel()
	url := natstest.Start(t)
	stream := createStream(t, url, "ce")

	sink, err := cesink.New(domain.SinkSpec{Name: "out", From: "in", Spec: map[string]any{
		"broker": "nats",
		"nats":   map[string]any{"url": url},
		"destination": map[string]any{
			"logs": "'ce.' + logs.resource_logs[0].scope_logs[0].log_records[0].attributes.get_attr('team')",
		},
	}}, newValidator(), slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	t.Cleanup(func() { _ = sink.Stop() })

	logs := otlpgen.Logs(otlpgen.Shape{Records: 10, Resources: 1, Teams: 2, Attributes: 1})
	require.NoError(t, sink.Notify(t.Context(), &domain.Event{Logs: logs}))

	// One message per team, in binary content mode.
	info, err := stream.Info(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, 2, info.State.Msgs)
	msg, err := stream.GetLastMsgForSubject(t.Context(), "ce.team-1")
	require.NoError(t, err)
	assert.Equal(t, "1.0", msg.Header.Get("ce-specversion"))
	assert.Equal(t, "io.opentelemetry.logs", msg.Header.Get("ce-type"))
	assert.Equal(t, "tram/out", msg.Header.Get("ce-source"))
	assert.Equal(t, "ce.team-1", msg.Header.Get("ce-subject"))
	assert.NotEmpty(t, msg.Header.Get("ce-id"))
	// As the NATS binding spec asks, not as ce-datacontenttype.
	assert.Equal(t, "application/x-protobuf", msg.Header.Get("content-type"))
	assert.Empty(t, msg.Header.Get("ce-datacontenttype"))

	c := &collector{}
	run(t, newSource(t, url, "ce.>", c))
	require.Eventually(t, func() bool { return c.count() == 2 }, 10*time.Second, 50*time.Millisecond)

	records := 0
	c.mu.Lock()
	for _, e := range c.events {
		records += len(e.Logs.GetResourceLogs()[0].GetScopeLogs()[0].GetLogRecords())
	}
	c.mu.Unlock()
	assert.Equal(t, 10, records)

	require.Eventually(t, func() bool {
		info, err = stream.Info(t.Context())
		return err == nil && info.State.Msgs == 2
	}, 5*time.Second, 50*time.Millisecond)
	consumer, err := stream.Consumer(t.Context(), "test")
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		ci, ciErr := consumer.Info(t.Context())
		return ciErr == nil && ci.AckFloor.Stream == 2 && ci.NumAckPending == 0
	}, 5*time.Second, 50*time.Millisecond, "both messages are acknowledged")
}

func TestRejectsInvalidMessages(t *testing.T) {
	t.Parallel()
	url := natstest.Start(t)
	stream := createStream(t, url, "bad")

	nc, err := nats.Connect(url)
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	require.NoError(t, err)

	tests := []struct {
		subject string
		header  nats.Header
		data    []byte
	}{
		// Not a CloudEvent at all.
		{subject: "bad.plain", data: []byte("hello")},
		// A CloudEvent of another type.
		{subject: "bad.type", header: nats.Header{
			"ce-specversion": {"1.0"}, "ce-id": {"1"}, "ce-source": {"x"}, "ce-type": {"com.example"},
			"content-type": {"application/x-protobuf"},
		}},
		// OTLP type, but the data is not protobuf.
		{subject: "bad.data", header: nats.Header{
			"ce-specversion": {"1.0"}, "ce-id": {"2"}, "ce-source": {"x"}, "ce-type": {"io.opentelemetry.logs"},
			"content-type": {"application/x-protobuf"},
		}, data: []byte{0xff, 0xff, 0xff}},
	}
	for _, tt := range tests {
		_, err = js.PublishMsg(t.Context(), &nats.Msg{Subject: tt.subject, Header: tt.header, Data: tt.data})
		require.NoError(t, err)
	}

	c := &collector{}
	run(t, newSource(t, url, "bad.>", c))

	consumer, err := stream.Consumer(t.Context(), "test")
	require.Eventually(t, func() bool {
		if consumer == nil {
			consumer, err = stream.Consumer(t.Context(), "test")
			return false
		}
		ci, ciErr := consumer.Info(t.Context())
		return ciErr == nil && ci.AckFloor.Stream == uint64(len(tests)) && ci.NumAckPending == 0 &&
			ci.NumRedelivered == 0
	}, 10*time.Second, 50*time.Millisecond, "invalid messages are terminated, not redelivered")
	assert.Zero(t, c.count())
}

func TestRetriesFailedDeliveries(t *testing.T) {
	t.Parallel()
	url := natstest.Start(t)
	createStream(t, url, "retry")

	sink, err := cesink.New(domain.SinkSpec{Name: "out", From: "in", Spec: map[string]any{
		"broker":      "nats",
		"nats":        map[string]any{"url": url},
		"destination": map[string]any{"logs": "'retry.all'"},
	}}, newValidator(), slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	t.Cleanup(func() { _ = sink.Stop() })
	logs := otlpgen.Logs(otlpgen.Shape{Records: 1, Resources: 1, Teams: 1, Attributes: 1})
	require.NoError(t, sink.Notify(t.Context(), &domain.Event{Logs: logs}))

	c := &collector{err: assert.AnError}
	run(t, newSource(t, url, "retry.>", c))
	time.Sleep(500 * time.Millisecond)
	c.mu.Lock()
	c.err = nil
	c.mu.Unlock()

	// The redelivery delay is 5s.
	require.Eventually(t, func() bool { return c.count() == 1 }, 15*time.Second, 100*time.Millisecond)
}
