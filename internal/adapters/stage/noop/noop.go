package noop

import (
	"context"
	"log"

	"codeberg.org/eljoth/stream/internal/core/domain"
)

const (
	Type = "noop"
)

type NoOp struct {
	spec        domain.StageSpec
	subscribers []domain.Subscriber
}

func New(spec domain.StageSpec) *NoOp {
	return &NoOp{
		spec: spec,
	}
}

func (n *NoOp) Name() string {
	return n.spec.Name
}

func (n *NoOp) From() string {
	return n.spec.From
}

func (n *NoOp) Subscribe(sub domain.Subscriber) error {
	log.Printf("Subscribing %s to %s\n", sub.Name(), n.Name())
	n.subscribers = append(n.subscribers, sub)
	return nil
}

func (n *NoOp) Notify(ctx context.Context, event domain.Event) error {
	for _, subscriber := range n.subscribers {
		subscriber.Notify(ctx, event)
	}
	return nil
}
