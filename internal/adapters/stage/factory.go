package stage

import (
	"codeberg.org/eljoth/stream/internal/adapters/stage/noop"
	"codeberg.org/eljoth/stream/internal/core/domain"
	"github.com/go-playground/validator/v10"
)

type Factory struct {
	validator *validator.Validate
}

func NewFactory() *Factory {
	return &Factory{
		validator: validator.New(),
	}
}

func (f *Factory) New(spec domain.StageSpec) (domain.Stage, error) {
	switch spec.Type {
	case noop.Type:
		return noop.New(spec, f.validator)
	default:
		return nil, domain.ErrInvalidSourceType
	}
}
