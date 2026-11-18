// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package source

import (
	"fmt"
	"log/slog"

	"github.com/go-playground/validator/v10"

	contract "codeberg.org/eljoth/tram/internal/adapters/broker"
	kafkadriver "codeberg.org/eljoth/tram/internal/adapters/broker/kafka"
	natsdriver "codeberg.org/eljoth/tram/internal/adapters/broker/nats"
	"codeberg.org/eljoth/tram/internal/adapters/source/broker"
	"codeberg.org/eljoth/tram/internal/adapters/source/generator"
	"codeberg.org/eljoth/tram/internal/adapters/source/otlp"
	"codeberg.org/eljoth/tram/internal/core/domain"
)

type Factory struct {
	validator *validator.Validate
	logger    *slog.Logger
	drivers   contract.Drivers
}

func NewFactory(logger *slog.Logger) *Factory {
	v := validator.New(validator.WithRequiredStructEnabled())
	return &Factory{
		validator: v,
		logger:    logger,
		drivers: contract.Drivers{
			kafkadriver.Name: kafkadriver.New(v),
			natsdriver.Name:  natsdriver.New(v),
		},
	}
}

func (f *Factory) New(spec domain.SourceSpec) (domain.Source, error) {
	switch spec.Type {
	case broker.Type:
		return broker.New(spec, f.validator, f.logger, f.drivers)
	case generator.Type:
		return generator.New(spec, f.validator)
	case otlp.Type:
		return otlp.New(spec, f.validator, f.logger)
	default:
		return nil, fmt.Errorf("%w: %q", domain.ErrInvalidSourceType, spec.Type)
	}
}
