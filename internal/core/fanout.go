package core

import (
	"context"

	"golang.org/x/sync/errgroup"

	"codeberg.org/eljoth/tram/internal/core/domain"
)

// fanout is the only subscriber the core gives a source. It delivers each
// event to the source's children like base.Source does, so that the core can
// measure the event once, with the result the source's client gets.
type fanout struct {
	name     string
	children []domain.Subscriber
}

func (f *fanout) Name() string { return f.name }
func (f *fanout) From() string { return "" }

// Stop does nothing: the children are stopped through their nodes.
func (f *fanout) Stop() error { return nil }

func (f *fanout) Notify(ctx context.Context, event *domain.Event) error {
	// A single child needs no goroutine.
	if len(f.children) == 1 {
		return f.children[0].Notify(ctx, event)
	}
	eg := errgroup.Group{}
	for _, child := range f.children {
		eg.Go(func() error { return child.Notify(ctx, event) })
	}
	return eg.Wait()
}
