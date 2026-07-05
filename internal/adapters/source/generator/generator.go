package generator

import (
	"context"
	"time"

	"codeberg.org/eljoth/stream/internal/adapters/source/base"
	"codeberg.org/eljoth/stream/internal/core/domain"
	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"
)

const (
	Type = "generator"
)

type Generator struct {
	*base.Source[Spec]
	interval time.Duration
}

func New(source domain.SourceSpec, validator *validator.Validate) (*Generator, error) {
	base, err := base.New[Spec](source.Name, source.Spec, validator)
	if err != nil {
		return nil, err
	}

	interval, err := time.ParseDuration(base.Spec().Interval)
	if err != nil {
		return nil, err
	}

	return &Generator{
		Source:   base,
		interval: interval,
	}, nil
}

func (g *Generator) Run(ctx context.Context) error {
	for {
		select {
		case <-g.Context().Done():
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
