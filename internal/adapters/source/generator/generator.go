package generator

import (
	"context"
	"time"

	"github.com/go-playground/validator/v10"

	"codeberg.org/eljoth/tram/internal/adapters/source/base"
	"codeberg.org/eljoth/tram/internal/core/domain"
)

const (
	Type = "generator"
)

type Generator struct {
	*base.Source[Spec]

	interval time.Duration
	shutdown chan struct{}
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
		shutdown: make(chan struct{}),
	}, nil
}

func (g *Generator) Run(ctx context.Context) error {
	for {
		select {
		case <-g.shutdown:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(g.interval):
			event := &domain.Event{}
			_ = g.NotifySubscribers(ctx, event)
		}
	}
}

func (g *Generator) Stop() error {
	close(g.shutdown)
	return nil
}
