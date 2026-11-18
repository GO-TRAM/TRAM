// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package broker_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"codeberg.org/eljoth/tram/internal/adapters/broker"
	"codeberg.org/eljoth/tram/internal/adapters/broker/brokertest"
)

func TestDriversDriver(t *testing.T) {
	t.Parallel()

	m := brokertest.NewMemory(0)
	drivers := broker.Drivers[broker.PublisherDriver]{"memory": m}

	got, err := drivers.Driver("memory")
	require.NoError(t, err)
	assert.Same(t, m, got)

	_, err = drivers.Driver("kafka")
	require.ErrorIs(t, err, broker.ErrUnknownDriver)
	assert.Contains(t, err.Error(), "available: memory")
}

func TestOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		spec    map[string]any
		want    map[string]any
		wantErr bool
	}{
		{name: "options under the driver's name", spec: map[string]any{"nats": map[string]any{"url": "u"}},
			want: map[string]any{"url": "u"}},
		{name: "empty options", spec: map[string]any{"nats": map[string]any{}}, want: map[string]any{}},
		{name: "missing", spec: map[string]any{"kafka": map[string]any{}}, wantErr: true},
		{name: "not an object", spec: map[string]any{"nats": "nats://u"}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := broker.Options(tt.spec, "nats")
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
