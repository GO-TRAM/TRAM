package nats

import (
	"context"
	"log/slog"

	"github.com/go-playground/validator/v10"
	"github.com/nats-io/nats.go/jetstream"

	"codeberg.org/eljoth/tram/internal/adapters/source/base"
)

// NewWithoutConnection builds a source whose workers can run against a fake
// consumer, without a NATS connection.
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

// RunWorker runs one worker until CancelFetching is called.
func RunWorker(ctx context.Context, n *NATS, cons jetstream.Consumer) {
	n.worker(ctx, cons)
}

// CancelFetching stops the workers like Stop, without closing a connection.
func CancelFetching(n *NATS) {
	n.cancel()
}
