// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package nats

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/go-tram/tram/internal/adapters/broker"
)

// Without it, a publish whose acknowledgement is lost, e.g. while NATS
// restarts, would wait forever when the source's context has no deadline.
const publishAckTimeout = 10 * time.Second

type publisher struct {
	conn *nats.Conn
	js   jetstream.JetStream
}

func newPublisher(spec *PublisherSpec, cfg broker.PublisherConfig, logger *slog.Logger) (*publisher, error) {
	conn, err := connect(spec.URL, cfg.Name, logger)
	if err != nil {
		return nil, err
	}
	// The window bounds what NATS has to buffer: it stores the messages of a
	// stream one after another, so without a bound a fast sink can make it
	// run out of memory. At the bound, Publish waits, which slows the source
	// down.
	js, err := jetstream.New(conn,
		jetstream.WithPublishAsyncTimeout(publishAckTimeout),
		jetstream.WithPublishAsyncMaxPending(cfg.MaxPending))
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to initialize JetStream: %w", err)
	}
	return &publisher{conn: conn, js: js}, nil
}

func (p *publisher) CheckDestination(subject string) error {
	for token := range strings.SplitSeq(subject, ".") {
		switch {
		case token == "":
			return fmt.Errorf("%w: subject %q has an empty token", broker.ErrInvalidDestination, subject)
		case token == "*" || token == ">":
			return fmt.Errorf("%w: subject %q contains a wildcard", broker.ErrInvalidDestination, subject)
		case strings.ContainsAny(token, " \t\r\n"):
			return fmt.Errorf("%w: subject %q contains whitespace", broker.ErrInvalidDestination, subject)
		}
	}
	return nil
}

// MaxMessageSize is the server's max_payload. It bounds the whole message,
// so a body exactly at it is still refused for the protocol overhead; the
// check is meant for bodies far above it, e.g. after a large batch.
func (p *publisher) MaxMessageSize() int {
	return int(p.conn.MaxPayload())
}

func (p *publisher) Publish(ctx context.Context, m broker.Message) (broker.Ack, error) {
	if limit := p.MaxMessageSize(); limit > 0 && len(m.Body) > limit {
		return nil, fmt.Errorf("%w: %d bytes to %s, NATS accepts %d", broker.ErrMessageTooLarge,
			len(m.Body), m.Destination, limit)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	future, err := p.js.PublishMsgAsync(&nats.Msg{Subject: m.Destination, Data: m.Body},
		jetstream.WithStallWait(publishAckTimeout))
	if err != nil {
		return nil, err
	}
	return ack{future}, nil
}

func (p *publisher) Close() error {
	// Draining waits for publishes in flight.
	return p.conn.Drain()
}

type ack struct{ future jetstream.PubAckFuture }

func (a ack) Wait(ctx context.Context) error {
	select {
	case <-a.future.Ok():
		return nil
	case err := <-a.future.Err():
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
