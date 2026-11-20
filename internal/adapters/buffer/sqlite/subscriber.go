// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package sqlite

import (
	"context"
	"sync"
	"time"

	"github.com/go-tram/tram/internal/adapters/broker"
)

// subscriber reads the messages of one queue file. It takes one message at a
// time, so it buffers none besides the ones being delivered.
type subscriber struct {
	queue *queue
	path  string
	// unread uncounts the subscriber as a reader of the queue.
	unread    func()
	closeOnce sync.Once
}

var _ broker.Subscriber = (*subscriber)(nil)

// Open attaches to the queue; a file queue always exists once opened.
func (s *subscriber) Open(context.Context) error { return nil }

// Next returns the next due message, waiting for one to be published or
// retried, and returns only with a message or ctx's error.
func (s *subscriber) Next(ctx context.Context) (broker.Delivery, error) {
	for {
		entries, err := s.queue.lease(ctx, 1)
		if err != nil {
			return nil, err
		}
		if len(entries) > 0 {
			e := entries[0]
			return delivery{q: s.queue, entry: e, size: int64(len(e.payload))}, nil
		}

		at, ok, err := s.queue.nextDue(ctx)
		if err != nil {
			return nil, err
		}
		if !ok {
			// Nothing stored at all: wait for a write.
			select {
			case <-s.queue.wake:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			continue
		}
		delay := max(at.Sub(s.queue.now()), 0)
		timer := time.NewTimer(delay)
		select {
		case <-s.queue.wake:
			timer.Stop()
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		}
	}
}

// Close releases the queue; the file is closed once its last user is gone.
// Entries being delivered stay stored and come again after a restart.
// Close releases the queue once, however often it is called.
func (s *subscriber) Close() error {
	s.closeOnce.Do(func() {
		s.unread()
		queues.release(s.path)
	})
	return nil
}

// delivery is one leased message. It names its signal and counts its failed
// deliveries.
type delivery struct {
	q     *queue
	entry entry
	size  int64
}

var (
	_ broker.Delivery = delivery{}
	_ broker.Signaled = delivery{}
	_ broker.Counted  = delivery{}
)

func (d delivery) Body() []byte        { return d.entry.payload }
func (d delivery) Destination() string { return queueName(d.q.path) }
func (d delivery) Signal() string      { return d.entry.signal }
func (d delivery) Attempts() int       { return d.entry.attempts }

// Ack confirms the message: it is removed from the queue.
func (d delivery) Ack() error {
	return d.q.ack(context.Background(), d.entry.id, d.size)
}

// Retry has the message delivered again after delay.
func (d delivery) Retry(delay time.Duration) error {
	return d.q.retry(context.Background(), d.entry.id, delay)
}

// Reject drops the message for good.
func (d delivery) Reject() error {
	return d.q.reject(context.Background(), d.entry.id, d.size)
}
