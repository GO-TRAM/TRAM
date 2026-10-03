package core

import (
	"context"

	"golang.org/x/sync/errgroup"

	"codeberg.org/eljoth/tram/internal/core/domain"
)

// fanout is the only subscriber the core gives a source or stage. It delivers
// each event to the node's children like base.Source and base.Stage do, so
// that the core can count what the node passes on once, however many children
// it has.
type fanout struct {
	name     string
	children []domain.Subscriber
	out      recordCounter
}

func (f *fanout) Name() string { return f.name }
func (f *fanout) From() string { return "" }

// Stop does nothing: the children are stopped through their nodes.
func (f *fanout) Stop() error { return nil }

func (f *fanout) Notify(ctx context.Context, event *domain.Event) error {
	f.out.count(ctx, event)

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
