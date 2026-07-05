package generator

import (
	"context"
	"log"
	"time"

	"codeberg.org/eljoth/stream/internal/core/domain"
)

const (
	Type = "generator"
)

type Generator struct {
	ctx         context.Context
	cancel      context.CancelFunc
	spec        domain.SourceSpec
	subscribers []domain.Subscriber
}

func New(spec domain.SourceSpec) *Generator {
	ctx, cancel := context.WithCancel(context.Background())
	return &Generator{
		ctx:         ctx,
		cancel:      cancel,
		spec:        spec,
		subscribers: make([]domain.Subscriber, 0),
	}
}

func (g *Generator) Name() string {
	return g.spec.Name
}

func (g *Generator) Subscribe(sub domain.Subscriber) error {
	log.Printf("Subscribing %s to %s\n", sub.Name(), g.Name())
	g.subscribers = append(g.subscribers, sub)
	return nil
}

func (g *Generator) Run(ctx context.Context) error {
	log.Printf("Running source %s", g.Name())
	combinedCtx, stop := context.WithCancel(ctx)
	defer stop()

	for {
		select {
		case <-combinedCtx.Done():
		case <-g.ctx.Done():
			log.Printf("Stopping source %s", g.Name())
			return nil
		case <-time.After(10 * time.Second):
			for _, subscriber := range g.subscribers {
				subscriber.Notify(ctx, domain.Event{Metadata: map[string]string{"source": g.Name(), "subscriber": subscriber.Name()}})
			}
		}
	}
}

func (g *Generator) Stop() error {
	g.cancel()
	return nil
}
