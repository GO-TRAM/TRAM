// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package core_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/go-tram/tram/internal/core/domain"
)

func started(s *fakeSource) bool {
	select {
	case <-s.started:
		return true
	default:
		return false
	}
}

// A suspended stream is registered but its source does not run; resuming runs
// it, suspending again stops it.
func TestSuspendedStream(t *testing.T) {
	t.Parallel()

	c, reg := newCore(t)

	spec := streamSpec(1, sink("s", "fake"))
	spec.Suspended = true
	_, err := c.UpsertStream(t.Context(), spec)
	require.NoError(t, err)

	require.Equal(t, 1, reg.sourceCount(), "the source is built even when suspended")
	assert.False(t, started(reg.source(0)), "a suspended stream's source does not run")
	assert.Empty(t, reg.stoppedNames())

	got, err := c.GetStream(t.Context(), "stream")
	require.NoError(t, err)
	assert.True(t, got.Suspended)

	// Resume: the stream is rebuilt and runs.
	spec.Suspended = false
	_, err = c.UpsertStream(t.Context(), spec)
	require.NoError(t, err)
	require.Equal(t, 2, reg.sourceCount())
	require.Eventually(t, func() bool { return started(reg.source(1)) }, 5*time.Second, time.Millisecond)
	assert.Contains(t, reg.stoppedNames(), "source", "the suspended source is stopped on resume")

	got, err = c.GetStream(t.Context(), "stream")
	require.NoError(t, err)
	assert.False(t, got.Suspended)

	// Suspend again: the running source is stopped, the stream stays.
	spec.Suspended = true
	_, err = c.UpsertStream(t.Context(), spec)
	require.NoError(t, err)
	select {
	case <-reg.source(1).shutdown:
	default:
		t.Fatal("the running source was not stopped on suspend")
	}

	got, err = c.GetStream(t.Context(), "stream")
	require.NoError(t, err)
	assert.True(t, got.Suspended, "the stream stays registered while suspended")
}

// An unchanged suspended stream is not rebuilt on upsert.
func TestSuspendedStreamUpsertIsIdempotent(t *testing.T) {
	t.Parallel()

	c, reg := newCore(t)

	spec := streamSpec(1, sink("s", "fake"))
	spec.Suspended = true
	_, err := c.UpsertStream(t.Context(), spec)
	require.NoError(t, err)

	_, err = c.UpsertStream(t.Context(), spec)
	require.NoError(t, err)
	assert.Equal(t, 1, reg.sourceCount(), "an unchanged suspended stream is not rebuilt")
	assert.Empty(t, reg.stoppedNames())
}

// Deleting a suspended stream works although nothing runs.
func TestDeleteSuspendedStream(t *testing.T) {
	t.Parallel()

	c, reg := newCore(t)

	spec := streamSpec(1, sink("s", "fake"))
	spec.Suspended = true
	_, err := c.UpsertStream(t.Context(), spec)
	require.NoError(t, err)

	require.NoError(t, c.DeleteStream(t.Context(), "stream"))
	_, err = c.GetStream(t.Context(), "stream")
	require.ErrorIs(t, err, domain.ErrStreamNotFound)
	assert.Equal(t, 1, reg.sourceCount())
}
