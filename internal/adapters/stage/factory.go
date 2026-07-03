package stage

import "codeberg.org/eljoth/stream/internal/core/domain"

type Factory struct {
}

func NewFactory() *Factory {
	return &Factory{}
}

func (f *Factory) New(domain.StageSpec) (domain.Stage, error) {
	return nil, nil
}
