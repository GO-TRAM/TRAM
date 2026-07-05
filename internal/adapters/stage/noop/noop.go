// internal/adapters/stage/noop/noop.go
package noop

import (
	"context"

	"codeberg.org/eljoth/stream/internal/adapters/stage/base"
	"codeberg.org/eljoth/stream/internal/core/domain"
)

const Type = "noop"

type NoOp struct {
	*base.Stage
}

func New(spec domain.StageSpec) *NoOp {
	return &NoOp{
		Stage: base.New(spec.Name, spec.From),
	}
}

func (n *NoOp) Notify(ctx context.Context, event domain.Event) error {
	n.NotifySubscribers(ctx, event)
	return nil
}
