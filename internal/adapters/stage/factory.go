// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package stage

import (
	"fmt"
	"log/slog"

	"github.com/go-playground/validator/v10"

	"github.com/go-tram/tram/internal/adapters/driver"
	"github.com/go-tram/tram/internal/adapters/stage/batch"
	"github.com/go-tram/tram/internal/adapters/stage/decryption"
	"github.com/go-tram/tram/internal/adapters/stage/delay"
	"github.com/go-tram/tram/internal/adapters/stage/encryption"
	"github.com/go-tram/tram/internal/adapters/stage/filter"
	celfilter "github.com/go-tram/tram/internal/adapters/stage/filter/cel"
	"github.com/go-tram/tram/internal/adapters/stage/noop"
	"github.com/go-tram/tram/internal/adapters/stage/validation"
	celvalidation "github.com/go-tram/tram/internal/adapters/stage/validation/cel"
	"github.com/go-tram/tram/internal/adapters/stage/valuemap"
	"github.com/go-tram/tram/internal/core/domain"
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
	case decryption.Type:
		return decryption.New(spec, f.validator, f.logger)
	case delay.Type:
		return delay.New(spec, f.validator, f.logger)
	case encryption.Type:
		return encryption.New(spec, f.validator, f.logger)
	case batch.Type:
		return batch.New(spec, f.validator, f.logger)
	case valuemap.Type:
		return valuemap.New(spec, f.validator, f.logger)
	default:
		return nil, fmt.Errorf("%w: %q", domain.ErrInvalidStageType, spec.Type)
	}
}
