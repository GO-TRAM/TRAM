package core

import (
	"context"

	"golang.org/x/sync/errgroup"

	"codeberg.org/eljoth/tram/internal/core/domain"
)

// fanout stands in for a node's children, so what the node passes on is
// counted once, not once per child.
type fanout struct {
	name     string
	children []domain.Subscriber
	out      recordCounter
}

func (f *fanout) Name() string { return f.name }
func (f *fanout) From() string { return "" }

// Children are stopped through their own nodes.
func (f *fanout) Stop() error { return nil }

func (f *fanout) Notify(ctx context.Context, event *domain.Event) error {
	f.out.count(ctx, event)

	if len(f.children) == 1 {
		return f.children[0].Notify(ctx, event)
	}
	eg := errgroup.Group{}
	for _, child := range f.children {
		eg.Go(func() error { return child.Notify(ctx, event) })
	}
	return eg.Wait()
}
