// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package amqp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/Azure/go-amqp"

	"codeberg.org/tram/tram/internal/adapters/broker"
)

const (
	// How long the broker may take to settle a message. Like the publish
	// timeout of the NATS driver.
	sendTimeout = 10 * time.Second
	// How long Close waits for messages in flight.
	closeTimeout = 10 * time.Second
)

// publisher sends each message on the sender link of its address. A send
// waits for the broker's disposition, so the window runs sends concurrently:
// up to MaxPending at once, and Publish waits for a free slot.
type publisher struct {
	session  *session
	maxBytes int
	slots    chan struct{}
	wg       sync.WaitGroup

	mu      sync.Mutex
	senders map[string]sender
}

// sender is a link of one session generation.
type sender struct {
	link       *amqp.Sender
	generation int
}

func newPublisher(spec *PublisherSpec, cfg broker.PublisherConfig, logger *slog.Logger) *publisher {
	return &publisher{
		session:  newSession(spec.ConnectionSpec, cfg.Name, logger),
		maxBytes: spec.MaxMessageBytes,
		slots:    make(chan struct{}, cfg.MaxPending),
		senders:  map[string]sender{},
	}
}

// CheckDestination refuses only what no broker accepts: address formats
// differ between brokers, and the broker refuses the rest when the link is
// attached.
func (p *publisher) CheckDestination(address string) error {
	if address == "" {
		return fmt.Errorf("%w: empty address", broker.ErrInvalidDestination)
	}
	if strings.IndexFunc(address, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) >= 0 {
		return fmt.Errorf("%w: address %q contains whitespace", broker.ErrInvalidDestination, address)
	}
	return nil
}

func (p *publisher) MaxMessageSize() int {
	return p.maxBytes
}

func (p *publisher) Publish(ctx context.Context, m broker.Message) (broker.Ack, error) {
	if p.maxBytes > 0 && len(m.Body) > p.maxBytes {
		return nil, fmt.Errorf("%w: %d bytes to %s, the limit is %d", broker.ErrMessageTooLarge,
			len(m.Body), m.Destination, p.maxBytes)
	}
	select {
	case p.slots <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	s, err := p.sender(ctx, m.Destination)
	if err != nil {
		<-p.slots
		return nil, err
	}
	if limit := s.link.MaxMessageSize(); limit > 0 && uint64(len(m.Body)) > limit {
		<-p.slots
		return nil, fmt.Errorf("%w: %d bytes to %s, the broker accepts %d", broker.ErrMessageTooLarge,
			len(m.Body), m.Destination, limit)
	}

	done := make(chan error, 1)
	p.wg.Go(func() {
		defer func() { <-p.slots }()
		sendCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), sendTimeout)
		defer cancel()
		sendErr := s.link.Send(sendCtx, amqp.NewMessage(m.Body), nil)
		if sendErr != nil {
			p.drop(m.Destination, s, sendErr)
		}
		done <- sendErr
	})
	return ack(done), nil
}

// sender returns the sender link of address, attaching one if there is none
// for the current session.
func (p *publisher) sender(ctx context.Context, address string) (sender, error) {
	sess, generation, err := p.session.get(ctx)
	if err != nil {
		return sender{}, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if s, ok := p.senders[address]; ok && s.generation == generation {
		return s, nil
	}
	attachCtx, cancel := context.WithTimeout(ctx, connectTimeout)
	defer cancel()
	link, err := sess.NewSender(attachCtx, address, nil)
	if err != nil {
		p.session.fail(generation, err)
		return sender{}, fmt.Errorf("failed to attach to address %s: %w", address, err)
	}
	s := sender{link: link, generation: generation}
	p.senders[address] = s
	return s, nil
}

// drop forgets the link of address after a failed send, so the next message
// attaches a new one, and the session if the failure ended it.
func (p *publisher) drop(address string, s sender, err error) {
	p.session.fail(s.generation, err)
	var linkErr *amqp.LinkError
	if !errors.As(err, &linkErr) && !broken(err) {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if current, ok := p.senders[address]; ok && current.link == s.link {
		delete(p.senders, address)
	}
}

func (p *publisher) Close() error {
	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(closeTimeout):
	}
	return p.session.close()
}

type ack chan error

func (a ack) Wait(ctx context.Context) error {
	select {
	case err := <-a:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
