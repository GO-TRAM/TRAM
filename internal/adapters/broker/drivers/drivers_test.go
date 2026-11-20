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

	"codeberg.org/eljoth/tram/internal/adapters/broker"
	"codeberg.org/eljoth/tram/internal/adapters/broker/amqp"
	"codeberg.org/eljoth/tram/internal/adapters/broker/drivers"
	"codeberg.org/eljoth/tram/internal/adapters/broker/kafka"
	"codeberg.org/eljoth/tram/internal/adapters/broker/nats"
	"codeberg.org/eljoth/tram/internal/adapters/driver"
)

func TestFactories(t *testing.T) {
	t.Parallel()

	publishers := drivers.NewPublisherFactory(validator.New())
	subscribers := drivers.NewSubscriberFactory(validator.New())
	for name, want := range map[string]broker.Driver{
		amqp.Name:  &amqp.Driver{},
		kafka.Name: &kafka.Driver{},
		nats.Name:  &nats.Driver{},
	} {
		p, err := publishers.Driver(name)
		require.NoError(t, err, name)
		assert.IsType(t, want, p, name)

		s, err := subscribers.Driver(name)
		require.NoError(t, err, name)
		assert.IsType(t, want, s, name)
	}
}

func TestFactoriesUnknownDriver(t *testing.T) {
	t.Parallel()

	p, err := drivers.NewPublisherFactory(validator.New()).Driver("memory")
	require.ErrorIs(t, err, driver.ErrUnknown)
	require.ErrorContains(t, err, "available: amqp, kafka, nats")
	assert.Nil(t, p)

	s, err := drivers.NewSubscriberFactory(validator.New()).Driver("memory")
	require.ErrorIs(t, err, driver.ErrUnknown)
	assert.Nil(t, s)
}
