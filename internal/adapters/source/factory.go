// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package source

import (
	"fmt"
	"log/slog"

	"github.com/go-playground/validator/v10"

	"codeberg.org/tram/tram/internal/adapters/source/broker"
	"codeberg.org/tram/tram/internal/adapters/source/generator"
	"codeberg.org/tram/tram/internal/adapters/source/otlp"
	"codeberg.org/tram/tram/internal/core/domain"
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
	case broker.Type:
		return broker.New(spec, f.validator, f.logger)
	case generator.Type:
		return generator.New(spec, f.validator)
	case otlp.Type:
		return otlp.New(spec, f.validator, f.logger)
	default:
		return nil, fmt.Errorf("%w: %q", domain.ErrInvalidSourceType, spec.Type)
	}
}
