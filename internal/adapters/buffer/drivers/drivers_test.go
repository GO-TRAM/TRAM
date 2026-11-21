// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package drivers_test

import (
	"log/slog"
	"testing"

	"github.com/go-playground/validator/v10"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/go-tram/tram/internal/adapters/broker"
	"github.com/go-tram/tram/internal/adapters/buffer"
	"github.com/go-tram/tram/internal/adapters/buffer/drivers"
	"github.com/go-tram/tram/internal/adapters/buffer/sqlite"
	"github.com/go-tram/tram/internal/adapters/driver"
)

func testLogger() *slog.Logger { return slog.New(slog.DiscardHandler) }

func TestFactoryResolvesRegisteredDrivers(t *testing.T) {
	t.Parallel()

	f := drivers.NewFactory(validator.New(), t.TempDir())
	drv, err := f.Driver(sqlite.Name)
	require.NoError(t, err)
	require.NotNil(t, drv)

	_, err = f.Driver("nope")
	require.ErrorIs(t, err, driver.ErrUnknown)
	assert.Contains(t, err.Error(), sqlite.Name)

	assert.Len(t, f.All(), 1)
}

// Pending counts the queue files that still hold entries. An empty data
// directory has none; a queue with a stored message has one.
func TestPending(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	pending, err := drivers.Pending(t.Context(), dir, testLogger())
	require.NoError(t, err)
	assert.Zero(t, pending)

	drv := sqlite.New(validator.New(), dir)
	p, err := drv.NewPublisher(map[string]any{}, buffer.PublisherConfig{
		Name: "p", MaxPending: 1,
		Queue:  "outbox",
		MaxAge: drivers.DefaultMaxAge,
	}, testLogger())
	require.NoError(t, err)
	ack, err := p.Publish(t.Context(), broker.Message{Destination: "outbox", Body: []byte("x"), Signal: "logs"})
	require.NoError(t, err)
	require.NoError(t, ack.Wait(t.Context()))
	require.NoError(t, p.Close())

	pending, err = drivers.Pending(t.Context(), dir, testLogger())
	require.NoError(t, err)
	assert.Equal(t, 1, pending)
}
