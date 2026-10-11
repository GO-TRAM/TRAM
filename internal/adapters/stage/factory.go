// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package stage

import (
	"fmt"

	"github.com/go-playground/validator/v10"

	"codeberg.org/eljoth/tram/internal/adapters/stage/batch"
	"codeberg.org/eljoth/tram/internal/adapters/stage/celfilter"
	"codeberg.org/eljoth/tram/internal/adapters/stage/celvalidator"
	"codeberg.org/eljoth/tram/internal/adapters/stage/delay"
	"codeberg.org/eljoth/tram/internal/adapters/stage/noop"
	"codeberg.org/eljoth/tram/internal/core/domain"
)

type Factory struct {
	validator *validator.Validate
}

func NewFactory() *Factory {
	return &Factory{
		validator: validator.New(validator.WithRequiredStructEnabled()),
	}
}

func (f *Factory) New(spec domain.StageSpec) (domain.Stage, error) {
	switch spec.Type {
	case noop.Type:
		return noop.New(spec, f.validator)
	case celvalidator.Type:
		return celvalidator.New(spec, f.validator)
	case celfilter.Type:
		return celfilter.New(spec, f.validator)
	case delay.Type:
		return delay.New(spec, f.validator)
	case batch.Type:
		return batch.New(spec, f.validator)
	default:
		return nil, fmt.Errorf("%w: %q", domain.ErrInvalidStageType, spec.Type)
	}
}
