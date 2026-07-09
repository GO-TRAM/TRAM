package delay

import (
	"context"
	"time"

	"codeberg.org/eljoth/tram/internal/adapters/stage/base"
	"codeberg.org/eljoth/tram/internal/core/domain"
	"github.com/go-playground/validator/v10"
)

const Type = "delay"

type NoOp struct {
	*base.Stage[Spec]
	delay time.Duration
}

func New(stage domain.StageSpec, validator *validator.Validate) (*NoOp, error) {
	base, err := base.New[Spec](stage.Name, stage.From, stage.Spec, validator)
	if err != nil {
		return nil, err
	}
	delay, err := time.ParseDuration(base.Spec().Delay)
	if err != nil {
		return nil, err
	}
	return &NoOp{
		Stage: base,
		delay: delay,
	}, nil
}

func (n *NoOp) Notify(ctx context.Context, event *domain.Event) error {
	<-time.After(n.delay)
	return n.NotifySubscribers(ctx, event)
}
