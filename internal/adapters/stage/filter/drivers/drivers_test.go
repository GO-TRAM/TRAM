// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package drivers_test

import (
	"testing"

	"github.com/go-playground/validator/v10"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/go-tram/tram/internal/adapters/driver"
	"github.com/go-tram/tram/internal/adapters/stage/filter/cel"
	"github.com/go-tram/tram/internal/adapters/stage/filter/drivers"
)

func TestFactory(t *testing.T) {
	t.Parallel()

	drv, err := drivers.NewFactory(validator.New()).Driver(cel.Name)
	require.NoError(t, err)
	assert.IsType(t, &cel.Driver{}, drv)
}

func TestFactoryUnknownDriver(t *testing.T) {
	t.Parallel()

	drv, err := drivers.NewFactory(validator.New()).Driver("ottl")
	require.ErrorIs(t, err, driver.ErrUnknown)
	require.ErrorContains(t, err, "available: cel")
	assert.Nil(t, drv)
}
