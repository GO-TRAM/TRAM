// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package broker

import (
	"log/slog"

	"github.com/go-playground/validator/v10"

	contract "codeberg.org/tram/tram/internal/adapters/broker"
	"codeberg.org/tram/tram/internal/adapters/driver"
	"codeberg.org/tram/tram/internal/core/domain"
)

// NewWithDrivers exposes newSource to the tests, which inject the in-memory driver.
func NewWithDrivers(
	source domain.SourceSpec,
	validator *validator.Validate,
	logger *slog.Logger,
	driverFactory driver.Factory[contract.SubscriberDriver],
) (*Source, error) {
	return newSource(source, validator, logger, driverFactory)
}
