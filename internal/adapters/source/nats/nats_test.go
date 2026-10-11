// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package nats_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	natssource "codeberg.org/eljoth/tram/internal/adapters/source/nats"
	"codeberg.org/eljoth/tram/internal/core/domain"
	"codeberg.org/eljoth/tram/internal/testutil/otlpgen"
)

type fakeConsumer struct {
	jetstream.Consumer

	fetches atomic.Int64
	fetch   func(batch int) (jetstream.MessageBatch, error)
}

func (c *fakeConsumer) Fetch(batch int, _ ...jetstream.FetchOpt) (jetstream.MessageBatch, error) {
	c.fetches.Add(1)
	return c.fetch(batch)
}

type endedBatch struct{ err error }

func (b endedBatch) Messages() <-chan jetstream.Msg {
	ch := make(chan jetstream.Msg)
	close(ch)
	return ch
}

func (b endedBatch) Error() error { return b.err }

type fullBatch struct{ ch chan jetstream.Msg }

func newFullBatch(msgs []jetstream.Msg) fullBatch {
	ch := make(chan jetstream.Msg, len(msgs))
	for _, m := range msgs {
		ch <- m
	}
	close(ch)
	return fullBatch{ch: ch}
}

func (b fullBatch) Messages() <-chan jetstream.Msg { return b.ch }
func (b fullBatch) Error() error                   { return nil }

type fakeMsg struct {
	jetstream.Msg

	data []byte
	acks *atomic.Int64
}

func (m fakeMsg) Headers() nats.Header {
	return nats.Header{natssource.SignalHeader: []string{string(domain.SignalLogs)}}
}
func (m fakeMsg) Data() []byte { return m.data }
func (m fakeMsg) Ack() error   { m.acks.Add(1); return nil }

type queue struct {
	t    *testing.T
	data []byte
	acks atomic.Int64

	mu        sync.Mutex
	remaining int
	asked     []int
}

func newQueue(t *testing.T, total int) *queue {
	t.Helper()
	_, data, err := (&domain.Event{Logs: otlpgen.Logs(otlpgen.Shape{Records: 1, Resources: 1})}).MarshalOTLPData()
	require.NoError(t, err)
	return &queue{t: t, data: data, remaining: total}
}

func (q *queue) fetch(batch int) (jetstream.MessageBatch, error) {
	q.mu.Lock()
	q.asked = append(q.asked, batch)
	n := min(batch, q.remaining)
	q.remaining -= n
	q.mu.Unlock()

	if n == 0 {
		time.Sleep(5 * time.Millisecond)
		return endedBatch{err: context.DeadlineExceeded}, nil
	}
	msgs := make([]jetstream.Msg, n)
	for i := range msgs {
		msgs[i] = fakeMsg{data: q.data, acks: &q.acks}
	}
	return newFullBatch(msgs), nil
}

func (q *queue) askedFor() []int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return append([]int(nil), q.asked...)
}

type blockingSubscriber struct {
	proceed chan struct{}

	inFlight    atomic.Int64
	maxInFlight atomic.Int64
	delivered   atomic.Int64
}

func newBlockingSubscriber() *blockingSubscriber {
	return &blockingSubscriber{proceed: make(chan struct{})}
}

func (s *blockingSubscriber) Name() string { return "blocking" }
func (s *blockingSubscriber) From() string { return "nats" }
func (s *blockingSubscriber) Stop() error  { return nil }
func (s *blockingSubscriber) Notify(context.Context, *domain.Event) error {
	current := s.inFlight.Add(1)
	for {
		highest := s.maxInFlight.Load()
		if current <= highest || s.maxInFlight.CompareAndSwap(highest, current) {
			break
		}
	}
	<-s.proceed
	s.inFlight.Add(-1)
	s.delivered.Add(1)
	return nil
}

func consumer(
	t *testing.T,
	cons *fakeConsumer,
	sub *blockingSubscriber,
	workers int,
) (*natssource.NATS, chan struct{}) {
	t.Helper()
	src, err := natssource.NewWithoutConnection(slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	require.NoError(t, src.Subscribe(sub))

	done := make(chan struct{})
	go func() {
		defer close(done)
		natssource.Consume(t.Context(), src, cons, workers)
	}()
	return src, done
}

func runConsume(t *testing.T, cons *fakeConsumer, d time.Duration) string {
	t.Helper()

	var logs bytes.Buffer
	src, err := natssource.NewWithoutConnection(slog.New(slog.NewTextHandler(&logs, nil)))
	require.NoError(t, err)

	done := make(chan struct{})
	go func() {
		defer close(done)
		natssource.Consume(t.Context(), src, cons, 1)
	}()

	time.Sleep(d)
	natssource.CancelFetching(src)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("the source did not stop")
	}
	return logs.String()
}

func TestConsumeBacksOffAfterFailedFetches(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		fetch func(int) (jetstream.MessageBatch, error)
	}{
		{
			name:  "fetch fails",
			fetch: func(int) (jetstream.MessageBatch, error) { return nil, nats.ErrConnectionClosed },
		},
		{
			name: "batch ends with an error",
			fetch: func(int) (jetstream.MessageBatch, error) {
				return endedBatch{err: errors.New("consumer deleted")}, nil
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			cons := &fakeConsumer{fetch: c.fetch}
			logs := runConsume(t, cons, 350*time.Millisecond)

			// Waits of 100ms and 200ms allow three fetches in 350ms.
			assert.LessOrEqual(t, cons.fetches.Load(), int64(4), "the source spins")
			assert.GreaterOrEqual(t, cons.fetches.Load(), int64(2), "the source gives up")
			assert.Contains(t, logs, "Could not fetch messages")
		})
	}
}

func TestConsumeDoesNotBackOffAfterTimeout(t *testing.T) {
	t.Parallel()

	cons := &fakeConsumer{fetch: func(int) (jetstream.MessageBatch, error) {
		return endedBatch{err: context.DeadlineExceeded}, nil
	}}
	logs := runConsume(t, cons, 100*time.Millisecond)

	assert.Greater(t, cons.fetches.Load(), int64(10))
	assert.NotContains(t, logs, "Could not fetch messages")
}

func TestConsumeHandlesUpToWorkersMessagesAtOnce(t *testing.T) {
	t.Parallel()

	const workers, total = 3, 10
	q := newQueue(t, total)
	sub := newBlockingSubscriber()
	src, done := consumer(t, &fakeConsumer{fetch: q.fetch}, sub, workers)

	require.Eventually(t, func() bool { return sub.inFlight.Load() == workers }, time.Second, time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, int64(workers), sub.maxInFlight.Load(), "more messages than workers are handled")

	sub.proceed <- struct{}{}
	require.Eventually(t, func() bool {
		return sub.delivered.Load() == 1 && sub.inFlight.Load() == workers
	}, time.Second, time.Millisecond)

	close(sub.proceed)
	require.Eventually(t, func() bool { return q.acks.Load() == total }, time.Second, time.Millisecond)
	assert.Equal(t, int64(workers), sub.maxInFlight.Load())

	natssource.CancelFetching(src)
	<-done
}

func TestConsumeFetchesOnlyForFreeSlots(t *testing.T) {
	t.Parallel()

	const workers = 4
	q := newQueue(t, 2*workers)
	sub := newBlockingSubscriber()
	src, done := consumer(t, &fakeConsumer{fetch: q.fetch}, sub, workers)

	require.Eventually(t, func() bool { return sub.inFlight.Load() == workers }, time.Second, time.Millisecond)
	for range 2 {
		sub.proceed <- struct{}{}
	}
	require.Eventually(t, func() bool {
		return sub.delivered.Load() == 2 && sub.inFlight.Load() == workers
	}, time.Second, time.Millisecond)

	asked := q.askedFor()
	require.NotEmpty(t, asked)
	assert.Equal(t, workers, asked[0], "the first fetch takes all slots")
	for _, n := range asked[1:] {
		assert.LessOrEqual(t, n, 2, "a fetch asked for more messages than slots were free")
	}

	close(sub.proceed)
	natssource.CancelFetching(src)
	<-done
}

func TestConsumeWaitsForMessagesBeingHandled(t *testing.T) {
	t.Parallel()

	q := newQueue(t, 2)
	sub := newBlockingSubscriber()
	src, done := consumer(t, &fakeConsumer{fetch: q.fetch}, sub, 2)

	require.Eventually(t, func() bool { return sub.inFlight.Load() == 2 }, time.Second, time.Millisecond)
	natssource.CancelFetching(src)

	select {
	case <-done:
		t.Fatal("consume returned while messages were being handled")
	case <-time.After(100 * time.Millisecond):
	}

	close(sub.proceed)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("consume did not return after the messages were handled")
	}
	assert.Equal(t, int64(2), q.acks.Load())
}
