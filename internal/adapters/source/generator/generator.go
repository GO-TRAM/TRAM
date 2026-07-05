package generator

import (
	"context"
	"time"

	"codeberg.org/eljoth/stream/internal/adapters/source/base"
	"codeberg.org/eljoth/stream/internal/core/domain"
	"github.com/google/uuid"
)

const (
	Type = "generator"
)

type Generator struct {
	*base.Source
	interval time.Duration
}

func New(spec domain.SourceSpec) *Generator {
	return &Generator{
		Source:   base.New(spec.Name),
		interval: 10 * time.Second,
	}
}

func (g *Generator) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(g.interval):
			event := domain.Event{
				Metadata: map[string]string{},
				Data:     []byte(uuid.NewString()),
			}
			g.NotifySubscribers(ctx, event)
		}
	}
}

func (g *Generator) Stop() error {
	return nil
}
