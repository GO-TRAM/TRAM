// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package stage

import (
	"fmt"
	"log/slog"

	"github.com/go-playground/validator/v10"

	"codeberg.org/eljoth/tram/internal/adapters/driver"
	"codeberg.org/eljoth/tram/internal/adapters/stage/batch"
	"codeberg.org/eljoth/tram/internal/adapters/stage/delay"
	"codeberg.org/eljoth/tram/internal/adapters/stage/filter"
	celfilter "codeberg.org/eljoth/tram/internal/adapters/stage/filter/cel"
	"codeberg.org/eljoth/tram/internal/adapters/stage/noop"
	"codeberg.org/eljoth/tram/internal/adapters/stage/validation"
	celvalidation "codeberg.org/eljoth/tram/internal/adapters/stage/validation/cel"
	"codeberg.org/eljoth/tram/internal/adapters/stage/valuemap"
	"codeberg.org/eljoth/tram/internal/core/domain"
)

// Factory creates the stages. It is the one place that knows the concrete
// drivers of the stages that have them.
type Factory struct {
	validator         *validator.Validate
	logger            *slog.Logger
	filterDrivers     driver.Set[filter.Driver]
	validationDrivers driver.Set[validation.Driver]
}

func NewFactory(logger *slog.Logger) *Factory {
	v := validator.New(validator.WithRequiredStructEnabled())
	return &Factory{
		validator:         v,
		logger:            logger,
		filterDrivers:     driver.Set[filter.Driver]{celfilter.Name: celfilter.New(v)},
		validationDrivers: driver.Set[validation.Driver]{celvalidation.Name: celvalidation.New(v)},
	}
}

func (f *Factory) New(spec domain.StageSpec) (domain.Stage, error) {
	switch spec.Type {
	case noop.Type:
		return noop.New(spec, f.validator, f.logger)
	case validation.Type:
		return validation.New(spec, f.validator, f.logger, f.validationDrivers)
	case filter.Type:
		return filter.New(spec, f.validator, f.logger, f.filterDrivers)
	case delay.Type:
		return delay.New(spec, f.validator, f.logger)
	case batch.Type:
		return batch.New(spec, f.validator, f.logger)
	case valuemap.Type:
		return valuemap.New(spec, f.validator, f.logger)
	default:
		return nil, fmt.Errorf("%w: %q", domain.ErrInvalidStageType, spec.Type)
	}
}
