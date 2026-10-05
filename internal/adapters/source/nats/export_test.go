package nats

import (
	"context"
	"log/slog"

	"github.com/go-playground/validator/v10"
	"github.com/nats-io/nats.go/jetstream"

	"codeberg.org/eljoth/tram/internal/adapters/source/base"
)

// NewWithoutConnection builds a source that can consume from a fake consumer,
// without a NATS connection.
func NewWithoutConnection(logger *slog.Logger) (*NATS, error) {
	b, err := base.New[Spec]("nats", map[string]any{
		"url":     "nats://localhost:4222",
		"stream":  "test",
		"subject": "test.>",
	}, validator.New())
	if err != nil {
		return nil, err
	}
	stop, cancel := context.WithCancel(context.Background())
	return &NATS{Source: b, logger: logger, stop: stop, cancel: cancel}, nil
}

// Consume consumes with the given number of workers until CancelFetching is
// called and the messages being handled are done.
func Consume(ctx context.Context, n *NATS, cons jetstream.Consumer, workers int) {
	n.consume(ctx, cons, workers)
}

// CancelFetching stops fetching like Stop, without closing a connection.
func CancelFetching(n *NATS) {
	n.cancel()
}
