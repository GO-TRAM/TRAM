// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package stage_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"log/slog"

	"codeberg.org/tram/tram/internal/adapters/stage"
	"codeberg.org/tram/tram/internal/adapters/stage/filter"
	"codeberg.org/tram/tram/internal/adapters/stage/validation"
	"codeberg.org/tram/tram/internal/core/domain"
)

func TestFactoryUnknownType(t *testing.T) {
	t.Parallel()

	logger := slog.Default()
	_, err := stage.NewFactory(logger).New(domain.StageSpec{Name: "s", Type: "unknown"})
	require.ErrorIs(t, err, domain.ErrInvalidStageType)
	assert.ErrorContains(t, err, `"unknown"`)
}

func TestFactoryDrivers(t *testing.T) {
	t.Parallel()

	logs := map[string]any{"logs": "true"}
	cases := []struct {
		name      string
		spec      domain.StageSpec
		expectErr string
	}{
		{
			name: "filter with cel",
			spec: domain.StageSpec{Type: filter.Type, Spec: map[string]any{"driver": "cel", "cel": logs}},
		},
		{
			name: "validation with cel",
			spec: domain.StageSpec{Type: validation.Type, Spec: map[string]any{
				"driver": "cel", "on_mismatch": "drop", "cel": logs,
			}},
		},
		{
			name:      "filter without driver",
			spec:      domain.StageSpec{Type: filter.Type, Spec: map[string]any{"cel": logs}},
			expectErr: "driver is required",
		},
		{
			name:      "filter with unknown driver",
			spec:      domain.StageSpec{Type: filter.Type, Spec: map[string]any{"driver": "opa", "opa": logs}},
			expectErr: `unknown driver "opa", available: cel`,
		},
		{
			name:      "filter without driver options",
			spec:      domain.StageSpec{Type: filter.Type, Spec: map[string]any{"driver": "cel"}},
			expectErr: `driver cel needs its options under "cel"`,
		},
		{
			name: "filter with options of another driver",
			spec: domain.StageSpec{Type: filter.Type, Spec: map[string]any{
				"driver": "cel", "cel": logs, "opa": logs,
			}},
			expectErr: `unknown field "opa"`,
		},
		{
			name: "filter with expressions outside the driver options",
			spec: domain.StageSpec{
				Type: filter.Type,
				Spec: map[string]any{"driver": "cel", "cel": logs, "logs": "true"},
			},
			expectErr: `unknown field "logs"`,
		},
		{
			name:      "validation without on_mismatch",
			spec:      domain.StageSpec{Type: validation.Type, Spec: map[string]any{"driver": "cel", "cel": logs}},
			expectErr: "OnMismatch",
		},
		{
			name: "validation with on_mismatch in the driver options",
			spec: domain.StageSpec{Type: validation.Type, Spec: map[string]any{
				"driver": "cel", "on_mismatch": "drop", "cel": map[string]any{"logs": "true", "on_mismatch": "drop"},
			}},
			expectErr: `unknown field "on_mismatch"`,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			_, err := stage.NewFactory(slog.Default()).New(c.spec)
			if c.expectErr == "" {
				require.NoError(t, err)
				return
			}
			assert.ErrorContains(t, err, c.expectErr)
		})
	}
}
