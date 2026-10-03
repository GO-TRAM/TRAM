package source

import (
	"fmt"
	"log/slog"

	"github.com/go-playground/validator/v10"

	"codeberg.org/eljoth/tram/internal/adapters/source/generator"
	"codeberg.org/eljoth/tram/internal/adapters/source/nats"
	"codeberg.org/eljoth/tram/internal/adapters/source/otlp"
	"codeberg.org/eljoth/tram/internal/core/domain"
)

type Factory struct {
	validator *validator.Validate
	logger    *slog.Logger
}

func NewFactory(logger *slog.Logger) *Factory {
	return &Factory{
		validator: validator.New(validator.WithRequiredStructEnabled()),
		logger:    logger,
	}
}

func (f *Factory) New(spec domain.SourceSpec) (domain.Source, error) {
	switch spec.Type {
	case generator.Type:
		return generator.New(spec, f.validator)
	case nats.Type:
		return nats.New(spec, f.validator, f.logger)
	case otlp.Type:
		return otlp.New(spec, f.validator, f.logger)
	default:
		return nil, fmt.Errorf("%w: %q", domain.ErrInvalidSourceType, spec.Type)
	}
}
