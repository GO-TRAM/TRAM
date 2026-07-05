package sink

import (
	"codeberg.org/eljoth/stream/internal/adapters/sink/debug"
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

func (f *Factory) New(spec domain.SinkSpec) (domain.Sink, error) {
	switch spec.Type {
	case debug.Type:
		return debug.New(spec, f.validator)
	default:
		return nil, domain.ErrInvalidSourceType
	}
}
