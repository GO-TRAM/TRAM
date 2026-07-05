package debug

import (
	"context"
	"log"

	"codeberg.org/eljoth/stream/internal/adapters/sink/base"
	"codeberg.org/eljoth/stream/internal/core/domain"
)

const Type = "debug"

type Debug struct {
	*base.Sink
}

func New(spec domain.SinkSpec) *Debug {
	return &Debug{
		Sink: base.New(spec.Name, spec.From),
	}
}

func (d *Debug) Notify(ctx context.Context, event domain.Event) error {
	path := ctx.Value("path").(string)
	path += " -> " + d.Name()
	log.Printf("[DEBUG] %s: %s", path, event)
	return nil
}
