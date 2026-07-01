package sink

import "codeberg.org/eljoth/stream/internal/core/domain"

type Factory struct {
}

func (f *Factory) New(domain.SinkSpec) (domain.Sink, error) {
	return nil, nil
}
