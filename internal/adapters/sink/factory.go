// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package sink

import (
	"fmt"
	"log/slog"

	"github.com/go-playground/validator/v10"

	contract "codeberg.org/eljoth/tram/internal/adapters/broker"
	natsdriver "codeberg.org/eljoth/tram/internal/adapters/broker/nats"
	"codeberg.org/eljoth/tram/internal/adapters/sink/broker"
	"codeberg.org/eljoth/tram/internal/adapters/sink/debug"
	"codeberg.org/eljoth/tram/internal/adapters/sink/noop"
	"codeberg.org/eljoth/tram/internal/adapters/sink/otlp"
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
		drivers:   contract.Drivers{natsdriver.Name: natsdriver.New(v)},
	}
}

func (f *Factory) New(spec domain.SinkSpec) (domain.Sink, error) {
	switch spec.Type {
	case broker.Type:
		return broker.New(spec, f.validator, f.logger, f.drivers)
	case noop.Type:
		return noop.New(spec, f.validator, f.logger)
	case debug.Type:
		return debug.New(spec, f.validator, f.logger)
	case otlp.Type:
		return otlp.New(spec, f.validator, f.logger)
	default:
		return nil, fmt.Errorf("%w: %q", domain.ErrInvalidSinkType, spec.Type)
	}
}
