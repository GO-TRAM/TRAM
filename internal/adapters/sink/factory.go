package sink

import (
	"codeberg.org/eljoth/tram/internal/adapters/sink/debug"
	"codeberg.org/eljoth/tram/internal/adapters/sink/nats"
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

func (f *Factory) New(spec domain.SinkSpec) (domain.Sink, error) {
	switch spec.Type {
	case debug.Type:
		return debug.New(spec, f.validator)
	case nats.Type:
		return nats.New(spec, f.validator)
	default:
		return nil, domain.ErrInvalidSourceType
	}
}
