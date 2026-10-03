package generator

import (
	"context"
	"fmt"
	"sync"
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
	stopOnce sync.Once
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
	// A zero or negative interval would generate events in a busy loop.
	if interval <= 0 {
		return nil, fmt.Errorf("interval %s is not positive", base.Spec().Interval)
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

// Stop ends Run. It can be called more than once.
func (g *Generator) Stop() error {
	g.stopOnce.Do(func() { close(g.shutdown) })
	return nil
}
