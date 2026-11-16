// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package cloudevent_test

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cloudevents/sdk-go/v2/binding"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	cejs "github.com/cloudevents/sdk-go/protocol/nats_jetstream/v3"

	"codeberg.org/eljoth/tram/internal/adapters/cebroker"
	cesource "codeberg.org/eljoth/tram/internal/adapters/source/cloudevent"
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
func (c *countingSubscriber) From() string { return "ce" }
func (c *countingSubscriber) Stop() error  { return nil }
func (c *countingSubscriber) Notify(_ context.Context, e *domain.Event) error {
	if c.records.Add(int64(e.RecordCount())) >= c.want {
		c.once.Do(func() { close(c.done) })
	}
	return nil
}

// Mirrors BenchmarkConsume of the NATS source, so both can be compared.
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
			msg, msgErr := natsMessage(shape)
			if msgErr != nil {
				b.Fatal(msgErr)
			}
			b.Run(fmt.Sprintf("workers=%d/%s", workers, shape), func(b *testing.B) {
				run++
				consume(b, js, url, fmt.Sprintf("consume-%d", run), workers, shape.Records, msg)
			})
		}
	}
}

// natsMessage encodes a CloudEvent the way the binding sends it.
func natsMessage(shape otlpgen.Shape) (*nats.Msg, error) {
	signal, data, err := (&domain.Event{Logs: otlpgen.Logs(shape)}).MarshalOTLPData()
	if err != nil {
		return nil, err
	}
	e := cebroker.NewEvent("bench", "bench", signal, data)
	var body bytes.Buffer
	header, err := cejs.WriteMsg(binding.WithForceBinary(context.Background()), binding.ToMessage(e), &body)
	if err != nil {
		return nil, err
	}
	return &nats.Msg{Header: header, Data: body.Bytes()}, nil
}

func consume(b *testing.B, js jetstream.JetStream, url, name string, workers, records int, msg *nats.Msg) {
	subject := name + ".logs"
	ctx := context.Background()
	if _, createErr := js.CreateStream(
		ctx,
		jetstream.StreamConfig{Name: name, Subjects: []string{subject}},
	); createErr != nil {
		b.Fatal(createErr)
	}
	b.Cleanup(func() { _ = js.DeleteStream(context.Background(), name) })
	publish(b, js, subject, msg, b.N)

	src, newErr := cesource.New(domain.SourceSpec{Name: "ce", Spec: map[string]any{
		"broker":  "nats",
		"workers": float64(workers),
		"nats":    map[string]any{"url": url, "subject": subject},
	}}, newValidator(), slog.New(slog.DiscardHandler))
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

func publish(b *testing.B, js jetstream.JetStream, subject string, msg *nats.Msg, n int) {
	b.Helper()
	acks := make([]jetstream.PubAckFuture, 0, n)
	for range n {
		ack, err := js.PublishMsgAsync(&nats.Msg{Subject: subject, Data: msg.Data, Header: msg.Header})
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
