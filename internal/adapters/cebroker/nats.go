// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package cebroker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	cejs "github.com/cloudevents/sdk-go/protocol/nats_jetstream/v3"
	"github.com/cloudevents/sdk-go/v2/binding"
	"github.com/cloudevents/sdk-go/v2/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"codeberg.org/eljoth/tram/internal/adapters/natsconn"
)

const (
	// Without it, a publish whose acknowledgement is lost, e.g. while NATS
	// restarts, would wait forever when the source's context has no deadline.
	natsPublishTimeout = 10 * time.Second

	// NATS' default for a consumer's max ack pending.
	natsDefaultMaxAckPending = 1000
	// A worker's message being handled and one buffered for it.
	natsUnackedPerWorker = 2

	natsContentTypeHeader     = "content-type"
	natsDataContentTypeHeader = "ce-datacontenttype"
)

// NATSSinkSpec connects a sink to NATS JetStream. The streams have to exist:
// the broker is set up outside of Tram.
type NATSSinkSpec struct {
	URL string `json:"url" validate:"required"`
}

// NATSSourceSpec connects a source to NATS JetStream. The stream holding
// Subject has to exist; the source creates or updates its consumer.
type NATSSourceSpec struct {
	URL     string `json:"url"               validate:"required"`
	Subject string `json:"subject"           validate:"required"`
	Durable string `json:"durable,omitempty"`
}

type natsSender struct {
	conn *nats.Conn
	js   jetstream.JetStream
}

// NewNATSSender connects to NATS. name identifies the connection in NATS'
// monitoring.
func NewNATSSender(spec *NATSSinkSpec, name string, logger *slog.Logger) (Sender, error) {
	conn, err := natsconn.Connect(spec.URL, name, logger)
	if err != nil {
		return nil, err
	}
	js, err := jetstream.New(conn)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to initialize JetStream: %w", err)
	}
	return &natsSender{conn: conn, js: js}, nil
}

func (s *natsSender) CheckDestination(subject string) error {
	for token := range strings.SplitSeq(subject, ".") {
		switch {
		case token == "":
			return fmt.Errorf("subject %q has an empty token", subject)
		case token == "*" || token == ">":
			return fmt.Errorf("subject %q contains a wildcard", subject)
		case strings.ContainsAny(token, " \t\r\n"):
			return fmt.Errorf("subject %q contains whitespace", subject)
		}
	}
	return nil
}

// Send encodes m with the binding's writer, so the message is the same as the
// binding sends, but publishes it itself: the binding's Send asks JetStream for
// the stream of the subject before every publish, a second round trip that
// halves the throughput. A subject without a stream fails the publish anyway,
// as no stream acknowledges it.
func (s *natsSender) Send(ctx context.Context, subject string, m binding.Message) (err error) {
	defer func() {
		if finishErr := m.Finish(err); err == nil {
			err = finishErr
		}
	}()
	ctx, cancel := context.WithTimeout(ctx, natsPublishTimeout)
	defer cancel()

	var body bytes.Buffer
	header, err := cejs.WriteMsg(binding.WithForceBinary(ctx), m, &body)
	if err != nil {
		return fmt.Errorf("failed to encode CloudEvent: %w", err)
	}
	// The NATS binding spec maps datacontenttype to the content-type header;
	// the writer prefixes it like the other attributes. The binding reads both.
	if contentType := header.Get(natsDataContentTypeHeader); contentType != "" {
		header.Del(natsDataContentTypeHeader)
		header.Set(natsContentTypeHeader, contentType)
	}
	_, err = s.js.PublishMsg(ctx, &nats.Msg{Subject: subject, Header: header, Data: body.Bytes()})
	return err
}

func (s *natsSender) Close() error {
	// Draining waits for publishes in flight.
	return s.conn.Drain()
}

type natsReceiver struct {
	conn     *nats.Conn
	protocol *cejs.Protocol
}

// NewNATSReceiver connects to NATS and prepares a consumer of spec.Subject that
// buffers up to workers messages.
func NewNATSReceiver(spec *NATSSourceSpec, workers int, name string, logger *slog.Logger) (Receiver, error) {
	conn, err := natsconn.Connect(spec.URL, name, logger)
	if err != nil {
		return nil, err
	}
	p, err := cejs.New(context.Background(),
		cejs.WithConnection(conn),
		cejs.WithConsumerConfig(&jetstream.ConsumerConfig{
			Durable:       spec.Durable,
			AckPolicy:     jetstream.AckExplicitPolicy,
			DeliverPolicy: jetstream.DeliverAllPolicy,
			FilterSubject: spec.Subject,
			// A worker's message and one buffered for it are unacknowledged;
			// NATS stops delivering at max ack pending.
			MaxAckPending: max(natsUnackedPerWorker*workers, natsDefaultMaxAckPending),
		}),
		cejs.WithPullConsumerOptions([]jetstream.PullConsumeOpt{jetstream.PullMaxMessages(workers)}),
	)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to create CloudEvents NATS protocol: %w", err)
	}
	return &natsReceiver{conn: conn, protocol: p}, nil
}

func (r *natsReceiver) Open(ctx context.Context) error {
	return r.protocol.OpenInbound(ctx)
}

func (r *natsReceiver) Receive(ctx context.Context) (binding.Message, error) {
	return r.protocol.Receive(ctx)
}

func (r *natsReceiver) Ack(m binding.Message) error {
	return m.Finish(protocol.ResultACK)
}

// Retry and Reject use the JetStream message: the binding only acks and naks
// right away.
func (r *natsReceiver) Retry(m binding.Message, delay time.Duration) error {
	msg, err := jetStreamMsg(m)
	if err != nil {
		return err
	}
	return msg.NakWithDelay(delay)
}

func (r *natsReceiver) Reject(m binding.Message) error {
	msg, err := jetStreamMsg(m)
	if err != nil {
		return err
	}
	return msg.Term()
}

func (r *natsReceiver) Close() error {
	r.conn.Close()
	return nil
}

var errNotJetStream = errors.New("message is not a NATS JetStream message")

func jetStreamMsg(m binding.Message) (jetstream.Msg, error) {
	msg, ok := m.(*cejs.Message)
	if !ok || msg.Msg == nil {
		return nil, fmt.Errorf("%w: %T", errNotJetStream, m)
	}
	return msg.Msg, nil
}
