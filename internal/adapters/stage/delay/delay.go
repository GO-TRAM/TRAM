package delay

import (
	"context"
	"time"

	"codeberg.org/eljoth/tram/internal/adapters/stage/base"
	"codeberg.org/eljoth/tram/internal/core/domain"
	"github.com/go-playground/validator/v10"
)

const Type = "delay"

type Delay struct {
	*base.Stage[Spec]
	delay   time.Duration
	stopped bool
}

func New(stage domain.StageSpec, validator *validator.Validate) (*Delay, error) {
	base, err := base.New[Spec](stage.Name, stage.From, stage.Spec, validator)
	if err != nil {
		return nil, err
	}
	delay, err := time.ParseDuration(base.Spec().Delay)
	if err != nil {
		return nil, err
	}
	return &Delay{
		Stage: base,
		delay: delay,
	}, nil
}

func (d *Delay) Notify(ctx context.Context, event *domain.Event) error {
	if d.stopped {
		return ErrDelayStopped
	}
	<-time.After(d.delay)
	return d.NotifySubscribers(ctx, event)
}

func (d *Delay) Stop() error {
	d.stopped = true
	<-time.After(d.delay)
	return d.Stage.Stop()
}
