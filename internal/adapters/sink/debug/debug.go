package debug

import (
	"context"
	"log"

	"codeberg.org/eljoth/stream/internal/core/domain"
)

const (
	Type = "debug"
)

type Debug struct {
	spec domain.SinkSpec
}

func New(spec domain.SinkSpec) *Debug {
	return &Debug{
		spec: spec,
	}
}

func (d *Debug) Name() string {
	return d.spec.Name
}

func (d *Debug) From() string {
	return d.spec.From
}

func (d *Debug) Notify(_ context.Context, event domain.Event) error {
	log.Printf("%s: %s -> %s", d.Name(), event.Metadata["source"], event.Metadata["subscriber"])
	return nil
}
