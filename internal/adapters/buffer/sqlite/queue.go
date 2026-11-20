// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package sqlite

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/go-tram/tram/internal/adapters/buffer"
)

// cleanupInterval is how often a queue removes entries older than their
// max_age.
const cleanupInterval = time.Minute

// queues holds the queue of every file this process opened, so a publisher
// and a subscriber of the same queue share one store and its lock.
var queues = &registry{byPath: map[string]*queue{}} //nolint:gochecknoglobals // One per process, like the files.

type registry struct {
	mu     sync.Mutex
	byPath map[string]*queue
}

// acquire returns the queue of the file at path, opening it on first use.
func (r *registry) acquire(path string, now func() time.Time, logger *slog.Logger) (*queue, error) {
	// The lock is held while the file is opened, so concurrent callers for
	// the same path wait and share the queue instead of failing on the file
	// lock this process already holds. Opening is rare and takes milliseconds.
	r.mu.Lock()
	defer r.mu.Unlock()
	if q := r.byPath[path]; q != nil {
		q.refs++
		return q, nil
	}

	st, err := openStore(path)
	if err != nil {
		return nil, err
	}
	stats, err := st.stats(context.Background())
	if err != nil {
		return nil, errors.Join(fmt.Errorf("read %s: %w", path, err), st.close())
	}
	q := &queue{
		store:  st,
		path:   path,
		now:    now,
		logger: logger,
		wake:   make(chan struct{}, 1),
		stop:   make(chan struct{}),
		writes: make(chan *write, writeWindow),
		size:   stats.bytes,
		leased: map[int64]struct{}{},
		refs:   1,
	}
	q.wg.Go(q.writeLoop)
	q.wg.Go(q.cleanupLoop)
	r.byPath[path] = q
	return q, nil
}

// release drops one reference to the queue of path and closes it when the
// last one is gone.
func (r *registry) release(path string) {
	r.mu.Lock()
	q := r.byPath[path]
	if q == nil {
		r.mu.Unlock()
		return
	}
	q.refs--
	if q.refs > 0 {
		r.mu.Unlock()
		return
	}
	// Shut down under the lock: until the file lock is released, a new
	// acquire of the path must wait instead of failing on it.
	delete(r.byPath, path)
	q.shutdown()
	r.mu.Unlock()
}

// inUse reports whether this process holds the file at path.
func (r *registry) inUse(path string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.byPath[path]
	return ok
}

// write is one group of entries to store in one commit.
type write struct {
	entries []entry
	size    int64
	done    chan error
}

// writeWindow is how many commits may wait for the writer; a publisher at
// the bound waits, which slows its source down.
const writeWindow = 256

// queue is the messages of one file, shared by its publishers and
// subscribers in this process.
type queue struct {
	store  *store
	path   string
	now    func() time.Time
	logger *slog.Logger
	wg     sync.WaitGroup

	wake   chan struct{}
	stop   chan struct{}
	writes chan *write

	mu     sync.Mutex
	refs   int
	size   int64
	leased map[int64]struct{}
	closed bool
	// readers counts the subscribers of the queue in this process.
	readers int
	// unreadSince is when the queue was first seen holding entries without a
	// reader; zero otherwise.
	unreadSince time.Time
	// unreadWarned is when that was last logged.
	unreadWarned time.Time
}

// unreadWarnInterval is how often a queue that holds entries but has no
// reader logs it.
const unreadWarnInterval = 10 * time.Minute

// enqueue adds the entries to the queue's size and hands them to the writer,
// blocking while the write window is full. With maxBytes above zero it fails
// with buffer.ErrFull while the queue would hold more than that. The caller
// reports the write through the returned write's done channel.
func (q *queue) enqueue(ctx context.Context, entries []entry, size, maxBytes int64) (*write, error) {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return nil, errors.New("queue closed")
	}
	if maxBytes > 0 && q.size+size > maxBytes {
		held := q.size
		q.mu.Unlock()
		return nil, fmt.Errorf("%w: the queue holds %d of %d bytes", buffer.ErrFull, held, maxBytes)
	}
	q.size += size
	q.mu.Unlock()

	w := &write{entries: entries, size: size, done: make(chan error, 1)}
	select {
	case q.writes <- w:
		return w, nil
	case <-ctx.Done():
		q.mu.Lock()
		q.size -= size
		q.mu.Unlock()
		return nil, ctx.Err()
	}
}

// writeLoop stores what publishers pass on, taking whatever waits as one
// commit, so concurrent publishes share one sync to disk without a delay.
func (q *queue) writeLoop() {
	for w := range q.writes {
		group := []*write{w}
	collect:
		for len(group) < writeWindow {
			select {
			case next := <-q.writes:
				group = append(group, next)
			default:
				break collect
			}
		}
		q.commit(group)
	}
}

func (q *queue) commit(group []*write) {
	var entries []entry
	for _, w := range group {
		entries = append(entries, w.entries...)
	}
	err := q.store.append(context.Background(), entries)
	if err != nil {
		q.logger.Error("Could not store messages", slog.Int("messages", len(entries)), slog.Any("error", err))
		err = fmt.Errorf("store messages in %s: %w", q.path, err)
		for _, w := range group {
			q.mu.Lock()
			q.size -= w.size
			q.mu.Unlock()
		}
	}
	for _, w := range group {
		w.done <- err
	}
	q.signal()
}

// signal wakes subscribers without blocking.
func (q *queue) signal() {
	select {
	case q.wake <- struct{}{}:
	default:
	}
}

// cleanupLoop removes expired entries once a minute. It runs as long as the
// queue is open, also without a publisher, so a drained volume is emptied by
// its readers too.
func (q *queue) cleanupLoop() {
	ticker := time.NewTicker(cleanupInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			q.expire()
			q.reportUnread()
		case <-q.stop:
			return
		}
	}
}

// addReader counts a subscriber of the queue; done uncounts it.
func (q *queue) addReader() func() {
	q.mu.Lock()
	q.readers++
	q.mu.Unlock()
	return func() {
		q.mu.Lock()
		q.readers--
		q.mu.Unlock()
	}
}

// reportUnread warns, every unreadWarnInterval, about a queue that holds
// entries while no buffer source of the instance reads it, e.g. because the
// reading stream is missing, suspended or on another Engine. The entries
// wait until a reader comes or they expire.
func (q *queue) reportUnread() {
	q.mu.Lock()
	readers := q.readers
	q.mu.Unlock()
	if readers > 0 {
		q.unreadSince = time.Time{}
		return
	}
	st, err := q.store.stats(context.Background())
	if err != nil || st.entries == 0 {
		q.unreadSince = time.Time{}
		return
	}
	now := q.now()
	if q.unreadSince.IsZero() {
		q.unreadSince = now
	}
	if now.Sub(q.unreadWarned) < unreadWarnInterval {
		return
	}
	q.unreadWarned = now
	q.logger.Warn("Buffer queue holds entries, but no buffer source of the instance reads it",
		slog.String("queue", queueName(q.path)), slog.Int("entries", st.entries),
		slog.Time("unread_since", q.unreadSince), slog.Time("oldest", st.oldest))
}

// expire removes the entries older than their max_age that are not being
// delivered.
func (q *queue) expire() {
	ctx := context.Background()
	maxAge, ok, err := q.store.maxAge(ctx)
	if err != nil || !ok {
		return
	}
	n, size, err := q.store.expire(ctx, q.now().Add(-maxAge), q.exclude())
	if err != nil {
		q.logger.Error("Could not remove expired entries", slog.Any("error", err))
		return
	}
	if n == 0 {
		return
	}
	q.mu.Lock()
	q.size -= size
	q.mu.Unlock()
	q.logger.Warn(
		"Removed entries older than max_age",
		slog.String(
			"file",
			q.path,
		),
		slog.Int("entries", n),
		slog.Int64("bytes", size),
		slog.Duration("max_age", maxAge),
	)
}

// lease marks entries as being delivered and returns them.
func (q *queue) lease(ctx context.Context, limit int) ([]entry, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	entries, err := q.store.due(ctx, q.now(), limit, q.excludeLocked())
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		q.leased[e.id] = struct{}{}
	}
	return entries, nil
}

// releaseLease marks an entry as no longer being delivered.
func (q *queue) releaseLease(id int64) {
	q.mu.Lock()
	delete(q.leased, id)
	q.mu.Unlock()
}

func (q *queue) exclude() []int64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.excludeLocked()
}

func (q *queue) excludeLocked() []int64 {
	out := make([]int64, 0, len(q.leased))
	for id := range q.leased {
		out = append(out, id)
	}
	return out
}

// nextDue returns when the next entry not being delivered is due, and false
// without such entries.
func (q *queue) nextDue(ctx context.Context) (time.Time, bool, error) {
	return q.store.next(ctx, q.exclude())
}

// ack removes a delivered entry.
func (q *queue) ack(ctx context.Context, id int64, size int64) error {
	err := q.store.remove(ctx, id)
	if err == nil {
		q.mu.Lock()
		q.size -= size
		q.mu.Unlock()
	}
	q.releaseLease(id)
	return err
}

// retry schedules the next delivery of a failed entry.
func (q *queue) retry(ctx context.Context, id int64, delay time.Duration) error {
	err := q.store.retry(ctx, id, q.now().Add(delay))
	q.releaseLease(id)
	q.signal()
	return err
}

// reject removes an entry that must not be delivered again.
func (q *queue) reject(ctx context.Context, id int64, size int64) error {
	return q.ack(ctx, id, size)
}

// shutdown stops the loops and closes the store.
func (q *queue) shutdown() {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return
	}
	q.closed = true
	q.mu.Unlock()
	close(q.stop)
	close(q.writes)
	q.wg.Wait()
	if err := q.store.close(); err != nil {
		q.logger.Error("Could not close buffer file", slog.String("file", q.path), slog.Any("error", err))
	}
}
