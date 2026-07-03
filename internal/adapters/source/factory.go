package source

import (
	"codeberg.org/eljoth/stream/internal/adapters/source/generator"
	"codeberg.org/eljoth/stream/internal/core/domain"
)

type Factory struct {
}

func NewFactory() *Factory {
	return &Factory{}
}

func (f *Factory) New(spec domain.SourceSpec) (domain.Source, error) {
	switch spec.Type {
	case generator.Type:
		return generator.New(spec), nil
	default:
		return nil, domain.ErrInvalidSourceType
	}
}
