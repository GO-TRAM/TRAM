package stage

import (
	"codeberg.org/eljoth/stream/internal/adapters/stage/noop"
	"codeberg.org/eljoth/stream/internal/core/domain"
)

type Factory struct {
}

func NewFactory() *Factory {
	return &Factory{}
}

func (f *Factory) New(spec domain.StageSpec) (domain.Stage, error) {
	switch spec.Type {
	case noop.Type:
		return noop.New(spec), nil
	default:
		return nil, domain.ErrInvalidSourceType
	}
}
