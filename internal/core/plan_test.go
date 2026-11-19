// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package core_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"codeberg.org/eljoth/tram/internal/core/domain"
)

func TestPlanStream(t *testing.T) {
	t.Parallel()

	c, reg := newCore(t)

	_, upsertErr := c.UpsertStream(t.Context(), streamSpec(1, sink("old", "fake")))
	require.NoError(t, upsertErr)

	t.Run("noop for an unchanged stream", func(t *testing.T) {
		t.Parallel()

		spec := streamSpec(1, sink("old", "fake"))
		plan, err := c.PlanStream(t.Context(), spec, spec)
		require.NoError(t, err)
		assert.Equal(t, domain.StreamPlanActionNoop, plan.Action)
		assert.False(t, plan.SourceReplaced)
		assert.Empty(t, plan.CreatedNodes)
		assert.Empty(t, plan.StoppedNodes)
	})

	t.Run("hot swap when only stages and sinks change", func(t *testing.T) {
		t.Parallel()

		spec := streamSpec(1, sink("new", "fake"))
		plan, err := c.PlanStream(t.Context(), spec, spec)
		require.NoError(t, err)
		assert.Equal(t, domain.StreamPlanActionHotSwap, plan.Action)
		assert.False(t, plan.SourceReplaced)
		assert.Equal(t, []string{"new"}, plan.CreatedNodes)
		assert.Equal(t, []string{"old"}, plan.StoppedNodes)

		got, err := c.GetStream(t.Context(), "stream")
		require.NoError(t, err)
		require.Len(t, got.Sinks, 1)
		assert.Equal(t, "old", got.Sinks[0].Name, "the dry run did not change the running stream")
	})

	t.Run("rebuild when the source changes", func(t *testing.T) {
		t.Parallel()

		spec := streamSpec(2, sink("new", "fake"))
		plan, err := c.PlanStream(t.Context(), spec, spec)
		require.NoError(t, err)
		assert.Equal(t, domain.StreamPlanActionRebuild, plan.Action)
		assert.True(t, plan.SourceReplaced)
		assert.Equal(t, []string{"new"}, plan.CreatedNodes)
		assert.Equal(t, []string{"old"}, plan.StoppedNodes)
	})

	t.Run("rebuild for a stream that does not exist", func(t *testing.T) {
		t.Parallel()

		// A fresh core: the fake source factory allows only one live source,
		// like a listening port.
		c2, _ := newCore(t)
		spec := streamSpec(1, sink("out", "fake"))
		spec.Name = "other"
		plan, err := c2.PlanStream(t.Context(), spec, spec)
		require.NoError(t, err)
		assert.Equal(t, domain.StreamPlanActionRebuild, plan.Action)
		assert.False(t, plan.SourceReplaced)
		assert.Equal(t, []string{"out"}, plan.CreatedNodes)
		assert.Empty(t, plan.StoppedNodes)
	})

	t.Run("invalid spec is rejected and creates nothing", func(t *testing.T) {
		t.Parallel()

		spec := streamSpec(1, sink("bad", brokenType))
		_, err := c.PlanStream(t.Context(), spec, spec)
		require.ErrorIs(t, err, domain.ErrStreamBuildFailed)
		assert.NotContains(t, reg.createdNames(), "bad", "the broken sink is never created")
	})
}
