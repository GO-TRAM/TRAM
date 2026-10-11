// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package delay_test

import (
	"context"
	"testing"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"codeberg.org/eljoth/tram/internal/adapters/stage/delay"
	"codeberg.org/eljoth/tram/internal/core/domain"
)

func TestNotifyReturnsEarly(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		interrupt func(d *delay.Delay, cancel context.CancelFunc)
		expected  error
	}{
		{
			name:      "context cancelled",
			interrupt: func(_ *delay.Delay, cancel context.CancelFunc) { cancel() },
			expected:  context.Canceled,
		},
		{
			name:      "stage stopped",
			interrupt: func(d *delay.Delay, _ context.CancelFunc) { _ = d.Stop() },
			expected:  delay.ErrDelayStopped,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			d, err := delay.New(domain.StageSpec{Name: "delay", Spec: map[string]any{"delay": "1h"}}, validator.New())
			require.NoError(t, err)

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			done := make(chan error, 1)
			go func() { done <- d.Notify(ctx, &domain.Event{}) }()
			c.interrupt(d, cancel)

			select {
			case notifyErr := <-done:
				require.ErrorIs(t, notifyErr, c.expected)
			case <-time.After(time.Second):
				t.Fatal("Notify did not return")
			}
		})
	}
}

func TestStopIsIdempotent(t *testing.T) {
	t.Parallel()

	d, err := delay.New(domain.StageSpec{Name: "delay", Spec: map[string]any{"delay": "1h"}}, validator.New())
	require.NoError(t, err)

	assert.NoError(t, d.Stop())
	assert.NoError(t, d.Stop())
}

func TestMaxHold(t *testing.T) {
	t.Parallel()

	d, err := delay.New(domain.StageSpec{Name: "delay", Spec: map[string]any{"delay": "3s"}}, validator.New())
	require.NoError(t, err)
	assert.Equal(t, 3*time.Second, d.MaxHold(), "every event is held for the delay")
}
