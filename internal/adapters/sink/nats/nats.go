package nats

import (
	"context"
	"fmt"
	"log"
	"sync"

	"codeberg.org/eljoth/tram/internal/adapters/sink/base"
	"codeberg.org/eljoth/tram/internal/core/domain"
	"github.com/go-playground/validator/v10"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	Type = "nats"
)

type NATS struct {
	*base.Sink[Spec]
	nc *nats.Conn
	js jetstream.JetStream
	wg sync.WaitGroup
	s  jetstream.Stream
}

func New(sink domain.SinkSpec, validator *validator.Validate) (*NATS, error) {
	base, err := base.New[Spec](sink.Name, sink.From, sink.Spec, validator)
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

	s, err := js.CreateOrUpdateStream(context.Background(), jetstream.StreamConfig{
		Name:     base.Spec().Stream,
		Subjects: []string{base.Spec().Subject},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create stream: %w", err)
	}

	return &NATS{
		Sink: base,
		nc:   nc,
		js:   js,
		s:    s,
	}, nil
}

func (d *NATS) Notify(ctx context.Context, event *domain.Event) error {
	data, err := event.MarshalOTLPData()
	if err != nil {
		log.Println(err)
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	_, err = d.js.Publish(ctx, d.Spec().Subject, data)
	if err != nil {
		log.Println(err)
		return fmt.Errorf("failed to publish event: %w", err)
	}

	return nil
}
