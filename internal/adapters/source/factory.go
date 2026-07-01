package source

import "codeberg.org/eljoth/stream/internal/core/domain"

type Factory struct {
}

func (f *Factory) New(domain.SourceSpec) (domain.Source, error) {
	return nil, nil
}
