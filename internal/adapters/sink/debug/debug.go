package debug

import (
	"context"
	"log"

	"codeberg.org/eljoth/stream/internal/adapters/sink/base"
	"codeberg.org/eljoth/stream/internal/core/domain"
	"github.com/go-playground/validator/v10"
)

const Type = "debug"

type Debug struct {
	*base.Sink[Spec]
}

func New(sink domain.SinkSpec, validator *validator.Validate) (*Debug, error) {
	base, err := base.New[Spec](sink.Name, sink.From, sink.Spec, validator)
	if err != nil {
		return nil, err
	}
	return &Debug{
		Sink: base,
	}, nil
}

func (d *Debug) Notify(ctx context.Context, event *domain.Event) error {
	path := ctx.Value(domain.ContextKeyPath).(string)
	path += " -> " + d.Name()
	log.Printf("[DEBUG] %s: %s", path, event)
	return nil
}
