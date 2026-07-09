package stage

import (
	"codeberg.org/eljoth/tram/internal/adapters/stage/delay"
	"codeberg.org/eljoth/tram/internal/adapters/stage/noop"
	"codeberg.org/eljoth/tram/internal/core/domain"
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
	case delay.Type:
		return delay.New(spec, f.validator)
	default:
		return nil, domain.ErrInvalidSourceType
	}
}
