// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package nats_test

import (
	"log/slog"

	natssource "codeberg.org/eljoth/tram/internal/adapters/source/nats"

	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"codeberg.org/eljoth/tram/internal/core/domain"
	"codeberg.org/eljoth/tram/internal/testutil/natstest"
	"codeberg.org/eljoth/tram/internal/testutil/otlpgen"
)

type countingSubscriber struct {
	records atomic.Int64
	want    int64
	done    chan struct{}
	once    sync.Once
}

func (c *countingSubscriber) Name() string { return "counter" }
func (c *countingSubscriber) From() string { return "nats" }
func (c *countingSubscriber) Stop() error  { return nil }
func (c *countingSubscriber) Notify(_ context.Context, e *domain.Event) error {
	if c.records.Add(int64(e.RecordCount())) >= c.want {
		c.once.Do(func() { close(c.done) })
	}
	return nil
}

func BenchmarkConsume(b *testing.B) {
	url := natstest.Start(b)

	nc, err := nats.Connect(url)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		b.Fatal(err)
	}

	shapes := []otlpgen.Shape{
		{Records: 1, Resources: 1, Teams: 1, Attributes: 4},
		{Records: 100, Resources: 1, Teams: 1, Attributes: 4},
		{Records: 1000, Resources: 10, Teams: 1, Attributes: 4},
	}

	run := 0
	for _, workers := range []int{1, 8} {
		for _, shape := range shapes {
			_, payload, marshalErr := (&domain.Event{Logs: otlpgen.Logs(shape)}).MarshalOTLPData()
			if marshalErr != nil {
				b.Fatal(marshalErr)
			}

			b.Run(fmt.Sprintf("workers=%d/%s", workers, shape), func(b *testing.B) {
				run++
				consume(b, js, url, fmt.Sprintf("consume-%d", run), workers, shape.Records, payload)
			})
		}
	}
}

func consume(b *testing.B, js jetstream.JetStream, url, name string, workers, records int, payload []byte) {
	subject := name + ".logs"
	ctx := context.Background()
	if _, createErr := js.CreateStream(
		ctx,
		jetstream.StreamConfig{Name: name, Subjects: []string{subject}},
	); createErr != nil {
		b.Fatal(createErr)
	}
	b.Cleanup(func() { _ = js.DeleteStream(context.Background(), name) })
	publish(b, js, subject, payload, b.N)

	src, newErr := natssource.New(domain.SourceSpec{Name: "nats", Spec: map[string]any{
		"url":     url,
		"stream":  name,
		"subject": subject,
		"workers": float64(workers),
	}}, validator.New(), slog.New(slog.DiscardHandler))
	if newErr != nil {
		b.Fatal(newErr)
	}
	counter := &countingSubscriber{want: int64(records * b.N), done: make(chan struct{})}
	_ = src.Subscribe(counter)

	b.ReportAllocs()
	b.ResetTimer()
	stopped := make(chan error, 1)
	go func() { stopped <- src.Run(ctx) }()
	select {
	case <-counter.done:
	case <-time.After(time.Minute):
		b.Fatalf("received %d of %d records", counter.records.Load(), counter.want)
	}
	b.StopTimer()

	_ = src.Stop()
	if runErr := <-stopped; runErr != nil {
		b.Fatal(runErr)
	}
	b.ReportMetric(float64(records*b.N)/b.Elapsed().Seconds(), "records/s")
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "messages/s")
}

func publish(b *testing.B, js jetstream.JetStream, subject string, payload []byte, n int) {
	b.Helper()
	acks := make([]jetstream.PubAckFuture, 0, n)
	for range n {
		ack, err := js.PublishMsgAsync(&nats.Msg{
			Subject: subject,
			Data:    payload,
			Header:  nats.Header{natssource.SignalHeader: []string{string(domain.SignalLogs)}},
		})
		if err != nil {
			b.Fatal(err)
		}
		acks = append(acks, ack)
	}
	for _, ack := range acks {
		select {
		case <-ack.Ok():
		case err := <-ack.Err():
			b.Fatal(err)
		}
	}
}
