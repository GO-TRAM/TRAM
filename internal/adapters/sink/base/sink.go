// internal/adapters/sink/base/sink.go
package base

import (
	"codeberg.org/eljoth/tram/internal/core/domain"
	"github.com/go-playground/validator/v10"
)

type Sink[T any] struct {
	name string
	from string
	spec *T
}

func New[T any](name, from string, spec map[string]any, validator *validator.Validate) (*Sink[T], error) {
	parsedSpec, err := domain.ParseSpec(spec, new(T))
	if err != nil {
		return nil, err
	}

	if err := validator.Struct(parsedSpec); err != nil {
		return nil, err
	}

	return &Sink[T]{
		name: name,
		from: from,
		spec: parsedSpec,
	}, nil
}

func (s *Sink[T]) Name() string {
	return s.name
}

func (s *Sink[T]) From() string {
	return s.from
}

func (s *Sink[T]) Spec() *T {
	return s.spec
}
