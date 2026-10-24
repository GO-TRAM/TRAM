// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package nats

import (
	"context"
	"log/slog"

	"github.com/go-playground/validator/v10"
	"github.com/nats-io/nats.go/jetstream"

	"codeberg.org/eljoth/tram/internal/adapters/source/base"
)

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
	return &NATS{
		Source:  b,
		stream:  "test",
		subject: "test.>",
		logger:  logger,
		stop:    stop,
		cancel:  cancel,
	}, nil
}

func Consume(ctx context.Context, n *NATS, cons jetstream.Consumer, workers int) {
	n.consume(ctx, cons, workers)
}

func ConsumerConfig(spec *Spec, workers int) jetstream.ConsumerConfig {
	return consumerConfig(spec, spec.Subject, workers)
}

type Target struct {
	Stream  string
	Subject string
}

// TargetOf returns the stream and subject the source with spec reads.
func TargetOf(spec map[string]any) (Target, error) {
	b, err := base.New[Spec]("nats", spec, validator.New())
	if err != nil {
		return Target{}, err
	}
	t, err := targetOf(b.Spec())
	return Target{Stream: t.stream, Subject: t.subject}, err
}

func CancelReceiving(n *NATS) {
	n.cancel()
}

// StopContext lets fake iterators end Next when the source stops, as
// jetstream.NextContext does.
func StopContext(n *NATS) context.Context {
	return n.stop
}
