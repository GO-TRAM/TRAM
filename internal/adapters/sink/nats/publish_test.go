// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package nats_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	natssink "codeberg.org/eljoth/tram/internal/adapters/sink/nats"
	"codeberg.org/eljoth/tram/internal/core/domain"
	"codeberg.org/eljoth/tram/internal/testutil/otlpgen"
)

type ackFuture struct {
	msg *nats.Msg
	ok  chan *jetstream.PubAck
	err chan error
}

func (f *ackFuture) Ok() <-chan *jetstream.PubAck { return f.ok }
func (f *ackFuture) Err() <-chan error            { return f.err }
func (f *ackFuture) Msg() *nats.Msg               { return f.msg }

// fakeJetStream acknowledges the published messages only once want of them
// are in flight, or fails them with ackErr. A sink that waits for each
// acknowledgement before publishing the next never gets there.
type fakeJetStream struct {
	jetstream.JetStream

	want   int
	ackErr error

	mu       sync.Mutex
	inFlight []*ackFuture
	subjects []string
}

func (f *fakeJetStream) PublishMsgAsync(msg *nats.Msg, _ ...jetstream.PublishOpt) (jetstream.PubAckFuture, error) {
	future := &ackFuture{msg: msg, ok: make(chan *jetstream.PubAck, 1), err: make(chan error, 1)}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.subjects = append(f.subjects, msg.Subject)
	f.inFlight = append(f.inFlight, future)
	if len(f.inFlight) == f.want {
		for _, pending := range f.inFlight {
			if f.ackErr != nil {
				pending.err <- f.ackErr
			} else {
				pending.ok <- &jetstream.PubAck{Stream: "test"}
			}
		}
		f.inFlight = nil
	}
	return future, nil
}

// teamSink publishes logs to one subject per team.
func teamSink(t *testing.T, js jetstream.JetStream) *natssink.NATS {
	t.Helper()
	sink, err := natssink.NewWithJetStream(map[string]any{
		"url":    "nats://localhost:4222",
		"stream": "test",
		"subject": map[string]any{
			"register": []any{">"},
			"logs":     "'logs.' + record.attributes.get_attr('team')",
		},
	}, validator.New(), js)
	require.NoError(t, err)
	return sink
}

func TestNotifyPublishesAllMessagesBeforeWaitingForAcknowledgements(t *testing.T) {
	t.Parallel()

	const teams = 20
	js := &fakeJetStream{want: teams}
	sink := teamSink(t, js)
	event := &domain.Event{Logs: otlpgen.Logs(otlpgen.Shape{Records: teams, Resources: 1, Teams: teams})}

	done := make(chan error, 1)
	go func() { done <- sink.Notify(t.Context(), event) }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Notify waited for an acknowledgement before publishing all messages")
	}
	assert.Len(t, js.subjects, teams)
}

func TestNotifyFailsIfAnAcknowledgementFails(t *testing.T) {
	t.Parallel()

	js := &fakeJetStream{want: 2, ackErr: errors.New("storage full")}
	sink := teamSink(t, js)
	event := &domain.Event{Logs: otlpgen.Logs(otlpgen.Shape{Records: 2, Resources: 1, Teams: 2})}

	err := sink.Notify(t.Context(), event)
	require.ErrorContains(t, err, "storage full")
	assert.NotErrorIs(t, err, domain.ErrEventRejected, "a failed publish is retried, not rejected")
}

func TestNotifyStopsWaitingWhenTheContextIsDone(t *testing.T) {
	t.Parallel()

	js := &fakeJetStream{want: 1000}
	sink := teamSink(t, js)
	event := &domain.Event{Logs: otlpgen.Logs(otlpgen.Shape{Records: 2, Resources: 1, Teams: 2})}

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	assert.ErrorIs(t, sink.Notify(ctx, event), context.DeadlineExceeded)
}
