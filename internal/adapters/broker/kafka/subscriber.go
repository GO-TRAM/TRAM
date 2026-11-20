// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/go-tram/tram/internal/adapters/broker"
)

const (
	// How often marked offsets are committed.
	commitInterval = time.Second
	// How long committing the last offsets may take when closing or when
	// partitions are taken away.
	commitTimeout = 10 * time.Second
)

var (
	errClosed  = errors.New("kafka subscriber closed")
	errNoTopic = errors.New("topic does not exist")
)

// subscriber reads one topic in a consumer group. Next is not safe for
// concurrent use; the source calls it from one goroutine.
type subscriber struct {
	spec     *SubscriberSpec
	capacity int
	logger   *slog.Logger
	client   *kgo.Client
	tracker  *tracker

	// buffer holds records polled but not yet handed out; only Next uses it.
	buffer []*entry

	// ready holds retried records whose delay is over; wake tells a waiting
	// Next about them.
	mu    sync.Mutex
	ready []*entry
	wake  chan struct{}
}

func newSubscriber(spec *SubscriberSpec, cfg broker.SubscriberConfig, logger *slog.Logger) (*subscriber, error) {
	s := &subscriber{spec: spec, capacity: cfg.Capacity, logger: logger, wake: make(chan struct{}, 1)}
	opts := append(connectionOptions(spec.ConnectionSpec, cfg.Name),
		kgo.ConsumerGroup(spec.Group),
		kgo.ConsumeTopics(spec.Topic),
		// Only offsets the tracker marked are committed, so a record is never
		// committed before it and all records before it are done.
		kgo.AutoCommitMarks(),
		kgo.AutoCommitInterval(commitInterval),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.OnPartitionsRevoked(s.revoked),
		kgo.OnPartitionsLost(s.lost),
	)
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka client: %w", err)
	}
	s.client = client
	s.tracker = newTracker(func(r *kgo.Record) { client.MarkCommitRecords(r) })
	return s, nil
}

// Open checks that the topic exists; the consumer group is joined by the first
// poll.
func (s *subscriber) Open(ctx context.Context) error {
	topics, err := kadm.NewClient(s.client).ListTopics(ctx, s.spec.Topic)
	if err != nil {
		return fmt.Errorf("failed to look up topic %s: %w", s.spec.Topic, err)
	}
	detail, ok := topics[s.spec.Topic]
	switch {
	case !ok:
		return fmt.Errorf("%w: %s", errNoTopic, s.spec.Topic)
	case detail.Err != nil:
		return fmt.Errorf("%w: %s: %w", errNoTopic, s.spec.Topic, detail.Err)
	}
	return nil
}

func (s *subscriber) Next(ctx context.Context) (broker.Delivery, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if e := s.popReady(); e != nil {
			return &delivery{s: s, e: e}, nil
		}
		if e := s.popBuffer(); e != nil {
			return &delivery{s: s, e: e}, nil
		}
		if err := s.poll(ctx); err != nil {
			return nil, err
		}
	}
}

// popReady returns a retried record whose partition is still assigned.
func (s *subscriber) popReady() *entry {
	s.mu.Lock()
	defer s.mu.Unlock()
	for len(s.ready) > 0 {
		e := s.ready[0]
		s.ready = s.ready[1:]
		if s.tracker.tracked(e) {
			return e
		}
	}
	return nil
}

// popBuffer returns a polled record whose partition is still assigned.
func (s *subscriber) popBuffer() *entry {
	for len(s.buffer) > 0 {
		e := s.buffer[0]
		s.buffer = s.buffer[1:]
		if s.tracker.tracked(e) {
			return e
		}
	}
	return nil
}

// poll fetches at most capacity records, waiting until there are some, a
// retried record is ready, or ctx ends.
func (s *subscriber) poll(ctx context.Context) error {
	pollCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <-s.wake:
			cancel()
		case <-pollCtx.Done():
		}
	}()

	fetches := s.client.PollRecords(pollCtx, s.capacity)
	if fetches.IsClientClosed() {
		return errClosed
	}
	fetches.EachError(func(topic string, partition int32, err error) {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return
		}
		s.logger.ErrorContext(ctx, "Could not fetch from Kafka, retrying", slog.String("topic", topic),
			slog.Int("partition", int(partition)), slog.Any("error", err))
	})
	fetches.EachRecord(func(r *kgo.Record) {
		s.buffer = append(s.buffer, s.tracker.add(r))
	})
	return nil
}

func (s *subscriber) retryAfter(e *entry, delay time.Duration) {
	time.AfterFunc(delay, func() {
		s.mu.Lock()
		s.ready = append(s.ready, e)
		s.mu.Unlock()
		select {
		case s.wake <- struct{}{}:
		default:
		}
	})
}

// revoked commits what is done of the partitions taken away, so their new
// owner continues there, and forgets them.
func (s *subscriber) revoked(ctx context.Context, cl *kgo.Client, partitions map[string][]int32) {
	commitCtx, cancel := context.WithTimeout(ctx, commitTimeout)
	defer cancel()
	if err := cl.CommitMarkedOffsets(commitCtx); err != nil {
		s.logger.WarnContext(ctx, "Could not commit offsets of revoked partitions", slog.Any("error", err))
	}
	s.tracker.revoke(partitions)
}

// lost forgets partitions that were taken away without a chance to commit.
func (s *subscriber) lost(_ context.Context, _ *kgo.Client, partitions map[string][]int32) {
	s.tracker.revoke(partitions)
}

// Close commits the offsets of finished records and leaves the group. Open
// records are read again by the next subscriber of the group.
func (s *subscriber) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), commitTimeout)
	defer cancel()
	err := s.client.CommitMarkedOffsets(ctx)
	s.client.Close()
	return err
}

// delivery is one record handed out. Retry hands it out again after the delay
// in this subscriber; its offset stays uncommitted until it is done, so a
// restart reads it again.
type delivery struct {
	s *subscriber
	e *entry
}

func (d *delivery) Body() []byte        { return d.e.record.Value }
func (d *delivery) Destination() string { return d.e.record.Topic }

func (d *delivery) Ack() error {
	d.s.tracker.finish(d.e)
	return nil
}

// Reject drops the record for good: its offset is committed like an
// acknowledged one.
func (d *delivery) Reject() error {
	return d.Ack()
}

func (d *delivery) Retry(delay time.Duration) error {
	d.s.retryAfter(d.e, delay)
	return nil
}
