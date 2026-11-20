// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package broker

import (
	"log/slog"

	"github.com/go-playground/validator/v10"

	contract "codeberg.org/eljoth/tram/internal/adapters/broker"
	"codeberg.org/eljoth/tram/internal/adapters/driver"
	"codeberg.org/eljoth/tram/internal/core/domain"
)

// NewWithDrivers exposes newSink to the tests, which inject the in-memory driver.
func NewWithDrivers(
	sink domain.SinkSpec,
	validator *validator.Validate,
	logger *slog.Logger,
	driverFactory driver.Factory[contract.PublisherDriver],
) (*Sink, error) {
	return newSink(sink, validator, logger, driverFactory)
}
