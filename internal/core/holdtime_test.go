// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package core_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/go-tram/tram/internal/core/domain"
)

func holdSpec(limit string, branches ...[]string) domain.StreamSpec {
	spec := domain.StreamSpec{
		Name:   "stream",
		Source: domain.SourceSpec{Name: "source", Type: "fake", Spec: map[string]any{}},
	}
	if limit != "" {
		spec.Source.Spec["delivery_timeout"] = limit
	}
	for b, holds := range branches {
		from := "source"
		for i, hold := range holds {
			name := string(rune('a'+b)) + string(rune('0'+i))
			spec.Stages = append(spec.Stages, domain.StageSpec{
				Name: name, Type: "fake", From: from, Spec: map[string]any{"hold": hold},
			})
			from = name
		}
		spec.Sinks = append(spec.Sinks, domain.SinkSpec{Name: "sink-" + string(rune('a'+b)), Type: "fake", From: from})
	}
	return spec
}

func TestUpsertChecksHoldTimes(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		spec     domain.StreamSpec
		rejected string
	}{
		{name: "below the limit", spec: holdSpec("8s", []string{"5s"})},
		{
			name:     "at the limit",
			spec:     holdSpec("8s", []string{"8s"}),
			rejected: "on the way to sink sink-a, a0 (8s) can hold an event for 8s, but source source cancels deliveries after 8s",
		},
		{
			name:     "stages on one path add up",
			spec:     holdSpec("8s", []string{"5s", "0s", "4s"}),
			rejected: "a0 (5s), a1 (0s), a2 (4s) can hold an event for 9s",
		},
		{name: "paths count separately", spec: holdSpec("8s", []string{"5s"}, []string{"5s"})},
		{
			name:     "any path can exceed",
			spec:     holdSpec("8s", []string{"1s"}, []string{"10s"}),
			rejected: "on the way to sink sink-b, b0 (10s)",
		},
		{name: "source without a limit", spec: holdSpec("", []string{"1h"})},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c, reg := newCore(t)
			_, err := c.UpsertStream(t.Context(), tc.spec)
			if tc.rejected == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, domain.ErrStreamBuildFailed)
			require.ErrorIs(t, err, domain.ErrInvalidStreamSpec)
			require.ErrorContains(t, err, tc.rejected)
			assert.Contains(t, reg.stoppedNames(), "source", "the components created are stopped again")
			_, err = c.GetStream(t.Context(), "stream")
			assert.ErrorIs(t, err, domain.ErrStreamNotFound)
		})
	}
}

func TestUpsertWithUnchangedSourceChecksHoldTimes(t *testing.T) {
	t.Parallel()

	c, reg := newCore(t)
	running := holdSpec("8s", []string{"5s"})
	_, err := c.UpsertStream(t.Context(), running)
	require.NoError(t, err)

	_, err = c.UpsertStream(t.Context(), holdSpec("8s", []string{"10s"}))
	require.ErrorIs(t, err, domain.ErrInvalidStreamSpec)

	got, err := c.GetStream(t.Context(), "stream")
	require.NoError(t, err)
	assert.True(t, running.Equal(*got), "the running stream is kept")
	assert.Equal(t, 1, reg.sourceCount(), "the source keeps running")
	assert.NotContains(t, reg.stoppedNames(), "source")
}
