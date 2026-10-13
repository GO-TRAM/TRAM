// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package nats_test

import (
	"bytes"
	"context"
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

// fakeConsumer hands out a new iterator for every call to Messages, whose Next
// is next. A nil next makes Messages fail.
type fakeConsumer struct {
	jetstream.Consumer

	iterators atomic.Int64
	stopped   atomic.Int64
	next      func(stop context.Context) (jetstream.Msg, error)
	stop      context.Context
}

func (c *fakeConsumer) Messages(...jetstream.PullMessagesOpt) (jetstream.MessagesContext, error) {
	c.iterators.Add(1)
	if c.next == nil {
		return nil, nats.ErrConnectionClosed
	}
	return &fakeIterator{consumer: c}, nil
}

type fakeIterator struct {
	consumer *fakeConsumer
}

func (i *fakeIterator) Next(...jetstream.NextOpt) (jetstream.Msg, error) {
	return i.consumer.next(i.consumer.stop)
}
func (i *fakeIterator) Stop()  { i.consumer.stopped.Add(1) }
func (i *fakeIterator) Drain() { i.consumer.stopped.Add(1) }

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
	data  []byte
	acks  atomic.Int64
	nexts atomic.Int64

	mu        sync.Mutex
	remaining int
}

func newQueue(t *testing.T, total int) *queue {
	t.Helper()
	_, data, err := (&domain.Event{Logs: otlpgen.Logs(otlpgen.Shape{Records: 1, Resources: 1})}).MarshalOTLPData()
	require.NoError(t, err)
	return &queue{data: data, remaining: total}
}

// next hands out the queued messages, then blocks like an iterator without
// pending messages until the source stops.
func (q *queue) next(stop context.Context) (jetstream.Msg, error) {
	q.nexts.Add(1)
	q.mu.Lock()
	if q.remaining > 0 {
		q.remaining--
		q.mu.Unlock()
		return fakeMsg{data: q.data, acks: &q.acks}, nil
	}
	q.mu.Unlock()
	<-stop.Done()
	return nil, stop.Err()
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
	cons.stop = natssource.StopContext(src)

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
	cons.stop = natssource.StopContext(src)

	done := make(chan struct{})
	go func() {
		defer close(done)
		natssource.Consume(t.Context(), src, cons, 1)
	}()

	time.Sleep(d)
	natssource.CancelReceiving(src)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("the source did not stop")
	}
	return logs.String()
}

func TestConsumeBacksOffAfterFailures(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		next func(context.Context) (jetstream.Msg, error)
	}{
		{
			name: "iterator cannot be created",
		},
		{
			name: "iterator ends with an error",
			next: func(context.Context) (jetstream.Msg, error) { return nil, jetstream.ErrConsumerDeleted },
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			cons := &fakeConsumer{next: c.next}
			logs := runConsume(t, cons, 350*time.Millisecond)

			// Waits of 100ms and 200ms allow three iterators in 350ms.
			assert.LessOrEqual(t, cons.iterators.Load(), int64(4), "the source spins")
			assert.GreaterOrEqual(t, cons.iterators.Load(), int64(2), "the source gives up")
			assert.Contains(t, logs, "Could not receive messages")
		})
	}
}

func TestConsumeKeepsIteratorAfterMissedHeartbeats(t *testing.T) {
	t.Parallel()

	var nexts atomic.Int64
	cons := &fakeConsumer{next: func(stop context.Context) (jetstream.Msg, error) {
		if nexts.Add(1) <= 3 {
			return nil, jetstream.ErrNoHeartbeat
		}
		<-stop.Done()
		return nil, stop.Err()
	}}
	logs := runConsume(t, cons, 50*time.Millisecond)

	assert.Equal(t, int64(1), cons.iterators.Load(), "missed heartbeats replaced the iterator")
	assert.Equal(t, int64(4), nexts.Load())
	assert.Contains(t, logs, "Missed heartbeats")
	assert.NotContains(t, logs, "Could not receive messages")
}

func TestConsumeStopsIterator(t *testing.T) {
	t.Parallel()

	cons := &fakeConsumer{next: func(stop context.Context) (jetstream.Msg, error) {
		<-stop.Done()
		return nil, stop.Err()
	}}
	logs := runConsume(t, cons, 20*time.Millisecond)

	assert.Equal(t, int64(1), cons.iterators.Load())
	assert.Equal(t, int64(1), cons.stopped.Load(), "the iterator was not stopped")
	assert.NotContains(t, logs, "Could not receive messages", "a stop counted as failure")
}

func TestConsumeHandlesUpToWorkersMessagesAtOnce(t *testing.T) {
	t.Parallel()

	const workers, total = 3, 10
	q := newQueue(t, total)
	sub := newBlockingSubscriber()
	src, done := consumer(t, &fakeConsumer{next: q.next}, sub, workers)

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

	natssource.CancelReceiving(src)
	<-done
}

func TestConsumeTakesMessagesOnlyForFreeWorkers(t *testing.T) {
	t.Parallel()

	const workers = 4
	q := newQueue(t, 2*workers)
	sub := newBlockingSubscriber()
	src, done := consumer(t, &fakeConsumer{next: q.next}, sub, workers)

	require.Eventually(t, func() bool { return sub.inFlight.Load() == workers }, time.Second, time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, int64(workers), q.nexts.Load(), "a message was taken while all workers were busy")

	for range 2 {
		sub.proceed <- struct{}{}
	}
	require.Eventually(t, func() bool {
		return sub.delivered.Load() == 2 && sub.inFlight.Load() == workers
	}, time.Second, time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, int64(workers+2), q.nexts.Load(), "more messages were taken than workers became free")

	close(sub.proceed)
	natssource.CancelReceiving(src)
	<-done
}

func TestConsumeWaitsForMessagesBeingHandled(t *testing.T) {
	t.Parallel()

	q := newQueue(t, 2)
	sub := newBlockingSubscriber()
	src, done := consumer(t, &fakeConsumer{next: q.next}, sub, 2)

	require.Eventually(t, func() bool { return sub.inFlight.Load() == 2 }, time.Second, time.Millisecond)
	natssource.CancelReceiving(src)

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
