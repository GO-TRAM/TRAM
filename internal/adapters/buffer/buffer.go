// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

// Package buffer is the contract between the buffer sink and source and the
// drivers that keep their queues in the data directory of the instance, e.g.
// sqlite. A buffer is a broker embedded in the instance: its drivers
// implement the broker contract, so the buffer source handles deliveries like
// the broker source, but the queue is a file on the instance's volume, which
// only streams of the same instance read.
package buffer

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/go-tram/tram/internal/adapters/broker"
)

var (
	// ErrNoDataDir is returned when the instance has no data directory, e.g.
	// an Engine without storage. Streams with a buffer do not run there.
	ErrNoDataDir = errors.New("the instance has no data directory for buffers")
	// ErrFull is returned by Publish while the queue holds max_bytes. The
	// buffer sink fails the event, and sources retry it later.
	ErrFull = errors.New("buffer full")
	// ErrInUse is returned when another process holds the queue's file.
	ErrInUse = errors.New("buffer file is in use by another process")
)

// PublisherConfig is what the buffer sink tells a driver about a publisher.
type PublisherConfig struct {
	broker.PublisherConfig

	// Queue is the queue the messages go to; the driver keeps it in the data
	// directory.
	Queue string
	// MaxAge is how long an entry waits for its delivery at most. It is a
	// property of the queue, kept with it, so readers and drains know it.
	MaxAge time.Duration
	// MaxBytes limits the size of the stored payloads.
	MaxBytes int64
}

// SubscriberConfig is what the buffer source tells a driver about a
// subscriber.
type SubscriberConfig struct {
	broker.SubscriberConfig

	// Queue is the queue the messages come from.
	Queue string
}

// PublisherDriver creates the publishers of one kind of buffer.
type PublisherDriver interface {
	NewPublisher(opts map[string]any, cfg PublisherConfig, logger *slog.Logger) (broker.Publisher, error)
}

// SubscriberDriver creates the subscribers of one kind of buffer. Its
// deliveries implement broker.Signaled and broker.Counted.
type SubscriberDriver interface {
	// NewSubscriber connects; the consumer is attached by Open.
	NewSubscriber(opts map[string]any, cfg SubscriberConfig, logger *slog.Logger) (broker.Subscriber, error)
}

// Driver serves both sides of one kind of buffer and lists its queues in the
// data directory.
type Driver interface {
	PublisherDriver
	SubscriberDriver
	// Files lists the queues in the data directory with what they hold,
	// without disturbing their readers and writers. Without a data
	// directory, there are none.
	Files(ctx context.Context) ([]File, error)
}

// File is a queue in the data directory.
type File struct {
	Path string
	// InUse tells whether a buffer of this process reads or writes it.
	InUse bool
	Info  Info
	// Err tells why the file could not be read; Info is empty then.
	Err error
}

// Info is what a queue holds.
type Info struct {
	Entries int
	// Newest is when the newest entry was created; zero without entries.
	Newest time.Time
	// MaxAge is the max_age the queue was last written with; zero if unknown.
	MaxAge time.Duration
}

// Expired reports whether every entry is older than the max_age, or than
// fallback if the queue does not know its max_age.
func (i Info) Expired(now time.Time, fallback time.Duration) bool {
	if i.Entries == 0 {
		return true
	}
	maxAge := i.MaxAge
	if maxAge <= 0 {
		maxAge = fallback
	}
	return now.Sub(i.Newest) > maxAge
}
