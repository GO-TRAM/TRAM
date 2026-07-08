package source

import (
	"codeberg.org/eljoth/stream/internal/adapters/source/generator"
	"codeberg.org/eljoth/stream/internal/adapters/source/otlp"
	"codeberg.org/eljoth/stream/internal/core/domain"
	"github.com/go-playground/validator/v10"
)

type Factory struct {
	validator *validator.Validate
}

func NewFactory() *Factory {
	return &Factory{
		validator: validator.New(validator.WithRequiredStructEnabled()),
	}
}

func (f *Factory) New(spec domain.SourceSpec) (domain.Source, error) {
	switch spec.Type {
	case generator.Type:
		return generator.New(spec, f.validator)
	case otlp.Type:
		return otlp.New(spec, f.validator)
	default:
		return nil, domain.ErrInvalidSourceType
	}
}
