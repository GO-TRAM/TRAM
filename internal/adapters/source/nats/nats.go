package nats

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"codeberg.org/eljoth/tram/internal/adapters/source/base"
	"codeberg.org/eljoth/tram/internal/core/domain"
	"github.com/go-playground/validator/v10"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	Type = "nats"
)

type NATS struct {
	*base.Source[Spec]
	nc       *nats.Conn
	js       jetstream.JetStream
	shutdown chan struct{}
	wg       sync.WaitGroup
}

func New(source domain.SourceSpec, validator *validator.Validate) (*NATS, error) {
	base, err := base.New[Spec](source.Name, source.Spec, validator)
	if err != nil {
		return nil, err
	}

	nc, err := nats.Connect(base.Spec().URL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize JetStream: %w", err)
	}

	return &NATS{
		Source:   base,
		nc:       nc,
		js:       js,
		shutdown: make(chan struct{}),
	}, nil
}

func (n *NATS) Run(ctx context.Context) error {
	stream, err := n.js.Stream(ctx, n.Spec().Stream)
	if err != nil {
		return fmt.Errorf("failed to get stream %s: %w", n.Spec().Stream, err)
	}

	cons, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:       n.Spec().Durable,
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		FilterSubject: n.Spec().Subject,
	})
	if err != nil {
		return fmt.Errorf("failed to create consumer: %w", err)
	}

	workers := uint(1)
	if n.Spec().Workers > 0 {
		workers = n.Spec().Workers
	}

	for range workers {
		n.wg.Add(1)
		go n.worker(ctx, cons)
	}

	<-n.shutdown
	n.wg.Wait()
	return nil
}

func (n *NATS) worker(ctx context.Context, cons jetstream.Consumer) {
	defer n.wg.Done()

	for {
		select {
		case <-n.shutdown:
			return
		default:
			msgs, err := cons.Fetch(10, jetstream.FetchMaxWait(5*time.Second))
			if err != nil {
				log.Printf("Error fetching messages: %v", err)
				continue
			}

			for msg := range msgs.Messages() {
				event, err := domain.UnmarshalOTLPData(msg.Data())
				if err != nil {
					log.Printf("Error parsing OTLP data: %v", err)
					continue
				}

				if err := n.NotifySubscribers(ctx, event); err != nil {
					log.Printf("Error notifying subscribers: %v", err)
				}

				if err := msg.Ack(); err != nil {
					log.Printf("Error acknowledging message: %v", err)
				}
			}
		}
	}
}

func (n *NATS) Stop() error {
	close(n.shutdown)
	n.nc.Close()
	return nil
}
