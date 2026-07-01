package core

import (
	"errors"

	"codeberg.org/eljoth/stream/internal/core/domain"
)

func (c *Core) BuildStream(
	spec domain.StreamSpec,
) (*domain.Stream, error) {
	if err := spec.Validate(); err != nil {
		return nil, errors.Join(domain.ErrInvalidStreamSpec, err)
	}

	return &domain.Stream{Spec: spec}, nil
}
