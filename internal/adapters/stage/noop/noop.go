// internal/adapters/stage/noop/noop.go
package noop

import (
	"context"

	"codeberg.org/eljoth/tram/internal/adapters/stage/base"
	"codeberg.org/eljoth/tram/internal/core/domain"
	"github.com/go-playground/validator/v10"
)

const Type = "noop"

type NoOp struct {
	*base.Stage[Spec]
}

func New(stage domain.StageSpec, validator *validator.Validate) (*NoOp, error) {
	base, err := base.New[Spec](stage.Name, stage.From, stage.Spec, validator)
	if err != nil {
		return nil, err
	}
	return &NoOp{
		Stage: base,
	}, nil
}

func (n *NoOp) Notify(ctx context.Context, event *domain.Event) error {
	n.NotifySubscribers(ctx, event)
	return nil
}
