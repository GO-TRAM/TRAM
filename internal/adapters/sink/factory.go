package sink

import (
	debug "codeberg.org/eljoth/stream/internal/adapters/sink/debug"
	"codeberg.org/eljoth/stream/internal/core/domain"
)

type Factory struct {
}

func NewFactory() *Factory {
	return &Factory{}
}

func (f *Factory) New(spec domain.SinkSpec) (domain.Sink, error) {
	switch spec.Type {
	case debug.Type:
		return debug.New(spec), nil
	default:
		return nil, domain.ErrInvalidSourceType
	}
}
