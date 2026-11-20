// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package kafka

import (
	"context"
	"fmt"
	"log/slog"
	"regexp"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/go-tram/tram/internal/adapters/broker"
)

const (
	// Kafka's default message.max.bytes is 1 MiB plus a little.
	defaultMaxMessageBytes = 1 << 20

	// How long a record may take to be acknowledged, including retries, e.g.
	// while a broker restarts. Like the publish timeout of the NATS driver.
	deliveryTimeout = 10 * time.Second

	// How long Close waits for records in flight.
	flushTimeout = 10 * time.Second

	// Kafka's limit on topic names.
	maxTopicLength = 249
)

var topicName = regexp.MustCompile(`^[a-zA-Z0-9._-]+$`)

type publisher struct {
	client   *kgo.Client
	maxBytes int
}

func newPublisher(spec *PublisherSpec, cfg broker.PublisherConfig, _ *slog.Logger) (*publisher, error) {
	maxBytes := spec.MaxMessageBytes
	if maxBytes == 0 {
		maxBytes = defaultMaxMessageBytes
	}
	opts := append(connectionOptions(spec.ConnectionSpec, cfg.Name),
		// The window: Produce blocks once this many records are not yet
		// acknowledged, which slows the source down.
		kgo.MaxBufferedRecords(cfg.MaxPending),
		kgo.ProducerBatchMaxBytes(int32(maxBytes)),
		kgo.RecordDeliveryTimeout(deliveryTimeout),
	)
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka client: %w", err)
	}
	return &publisher{client: client, maxBytes: maxBytes}, nil
}

func (p *publisher) CheckDestination(topic string) error {
	switch {
	case topic == "" || topic == "." || topic == "..":
		return fmt.Errorf("%w: topic %q", broker.ErrInvalidDestination, topic)
	case len(topic) > maxTopicLength:
		return fmt.Errorf("%w: topic %q is longer than %d characters", broker.ErrInvalidDestination, topic,
			maxTopicLength)
	case !topicName.MatchString(topic):
		return fmt.Errorf("%w: topic %q may only contain letters, digits, '.', '_' and '-'",
			broker.ErrInvalidDestination, topic)
	}
	return nil
}

// MaxMessageSize bounds the body; the record's overhead is small next to it.
func (p *publisher) MaxMessageSize() int {
	return p.maxBytes
}

func (p *publisher) Publish(ctx context.Context, m broker.Message) (broker.Ack, error) {
	if len(m.Body) > p.maxBytes {
		return nil, fmt.Errorf("%w: %d bytes to %s, the limit is %d", broker.ErrMessageTooLarge,
			len(m.Body), m.Destination, p.maxBytes)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	record := &kgo.Record{Topic: m.Destination, Value: m.Body}
	if m.Key != "" {
		record.Key = []byte(m.Key)
	}
	done := make(chan error, 1)
	p.client.Produce(ctx, record, func(_ *kgo.Record, err error) { done <- err })
	return ack(done), nil
}

func (p *publisher) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), flushTimeout)
	defer cancel()
	err := p.client.Flush(ctx)
	p.client.Close()
	return err
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
