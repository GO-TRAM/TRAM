// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package base

import (
	"github.com/go-playground/validator/v10"

	"github.com/go-tram/tram/internal/core/domain"
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

	if err = validator.Struct(parsedSpec); err != nil {
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

func (s *Sink[T]) Stop() error {
	return nil
}
