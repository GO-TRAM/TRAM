// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package buffer

import (
	"log/slog"

	"github.com/go-playground/validator/v10"

	"github.com/go-tram/tram/internal/adapters/buffer"
	"github.com/go-tram/tram/internal/adapters/driver"
	"github.com/go-tram/tram/internal/core/domain"
)

// NewWithDrivers exposes newSink to the tests, which inject a fake driver.
func NewWithDrivers(
	sink domain.SinkSpec,
	validator *validator.Validate,
	logger *slog.Logger,
	driverFactory driver.Factory[buffer.Driver],
) (*Sink, error) {
	return newSink(sink, validator, logger, driverFactory)
}
