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

	"github.com/go-tram/tram/internal/adapters/broker"
	"github.com/go-tram/tram/internal/adapters/buffer"
)

// publisher writes messages into queue files, one per destination.
type publisher struct {
	drv      *Driver
	maxBytes int64
	logger   *slog.Logger

	mu sync.Mutex
	// paths counts the queue references this publisher holds, one per open:
	// one for its configured queue and one per successfully published
	// destination.
	paths map[string]int
}

var _ broker.Publisher = (*publisher)(nil)

// CheckDestination reports whether queue is a valid queue name.
func (p *publisher) CheckDestination(queue string) error {
	if err := validateQueue(queue); err != nil {
		return fmt.Errorf("%w: %w", broker.ErrInvalidDestination, err)
	}
	return nil
}

// MaxMessageSize is the max_bytes of the queue: a larger message can never
// fit. Zero means no limit is known.
func (p *publisher) MaxMessageSize() int {
	if p.maxBytes <= 0 {
		return 0
	}
	return int(p.maxBytes)
}

// Publish stores the message in the queue its destination names and returns
// an ack that reports once it is durable. It fails with
// broker.ErrMessageTooLarge above max_bytes and with buffer.ErrFull while
// the queue holds max_bytes; Publish blocks while the write window is full.
func (p *publisher) Publish(ctx context.Context, m broker.Message) (broker.Ack, error) {
	if p.maxBytes > 0 && int64(len(m.Body)) > p.maxBytes {
		return nil, fmt.Errorf("%w: message of %d bytes is larger than max_bytes %d of queue %s",
			broker.ErrMessageTooLarge, len(m.Body), p.maxBytes, m.Destination)
	}
	path, err := p.drv.pathOf(m.Destination)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", broker.ErrInvalidDestination, err)
	}
	q, err := queues.acquire(path, p.drv.now, p.logger)
	if err != nil {
		return nil, err
	}
	size := int64(len(m.Body))
	w, err := q.enqueue(ctx, []entry{{signal: m.Signal, payload: m.Body, createdAt: q.now()}}, size, p.maxBytes)
	if err != nil {
		queues.release(path)
		if errors.Is(err, buffer.ErrFull) {
			return nil, fmt.Errorf("%w: queue %s is full", buffer.ErrFull, m.Destination)
		}
		return nil, err
	}
	p.mu.Lock()
	p.paths[path]++
	p.mu.Unlock()
	return ack{w: w, path: path}, nil
}

// Close releases every queue reference this publisher holds; a file is
// closed once its last user is gone.
func (p *publisher) Close() error {
	p.mu.Lock()
	paths := p.paths
	p.paths = map[string]int{}
	p.mu.Unlock()
	for path, refs := range paths {
		for range refs {
			queues.release(path)
		}
	}
	return nil
}

// ack is the answer to one published message.
type ack struct {
	w    *write
	path string
}

var _ broker.Ack = ack{}

// Wait returns once the message is stored durably, or with the reason it is
// not.
func (a ack) Wait(ctx context.Context) error {
	select {
	case err := <-a.w.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
