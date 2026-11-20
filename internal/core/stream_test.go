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

// Hot swapping: an update that keeps the source replaces only the stages and
// sinks, while the source keeps running.

func TestHotSwapRoutesEventsToTheNewSinks(t *testing.T) {
	t.Parallel()

	c, reg := newCore(t)
	_, err := c.UpsertStream(t.Context(), streamSpec(1, sink("old", "fake")))
	require.NoError(t, err)
	require.NoError(t, deliver(t, reg, logs(1)))

	_, err = c.UpsertStream(t.Context(), streamSpec(1, sink("new", "fake")))
	require.NoError(t, err)
	require.NoError(t, deliver(t, reg, logs(1)))
	require.NoError(t, deliver(t, reg, logs(1)))

	assert.Equal(t, 1, reg.sourceCount(), "the running source is kept")
	assert.Equal(t, 1, reg.receivedBy("old"), "the old sink got only the event before the swap")
	assert.Equal(t, 2, reg.receivedBy("new"), "the new sink gets every event after the swap")
	assert.Equal(t, []string{"old"}, reg.stoppedNames(), "only the replaced sink is stopped")
}

func TestHotSwapRoutesEventsThroughNewStagesToEveryBranch(t *testing.T) {
	t.Parallel()

	c, reg := newCore(t)
	_, err := c.UpsertStream(t.Context(), streamSpec(1, sink("old", "fake")))
	require.NoError(t, err)

	update := streamSpec(1,
		domain.SinkSpec{Name: "a", Type: "fake", From: "forward"},
		domain.SinkSpec{Name: "b", Type: "fake", From: "forward"},
		sink("direct", "fake"),
	)
	update.Stages = []domain.StageSpec{{Name: "forward", Type: forwardingType, From: "source"}}
	_, err = c.UpsertStream(t.Context(), update)
	require.NoError(t, err)
	require.NoError(t, deliver(t, reg, logs(1)))

	assert.Equal(t, 1, reg.sourceCount())
	for _, name := range []string{"a", "b", "direct"} {
		assert.Equal(t, 1, reg.receivedBy(name), "sink %s", name)
	}
	assert.Zero(t, reg.receivedBy("old"))
}

func TestFailedHotSwapKeepsDeliveringToTheOldSinks(t *testing.T) {
	t.Parallel()

	c, reg := newCore(t)
	_, err := c.UpsertStream(t.Context(), streamSpec(1, sink("old", "fake")))
	require.NoError(t, err)

	_, err = c.UpsertStream(t.Context(), streamSpec(1, sink("new", "fake"), sink("broken", brokenType)))
	require.ErrorIs(t, err, domain.ErrStreamBuildFailed)
	require.NoError(t, deliver(t, reg, logs(1)))

	assert.Equal(t, 1, reg.sourceCount(), "the source is kept")
	assert.Equal(t, 1, reg.receivedBy("old"), "events still reach the old sink")
	assert.Zero(t, reg.receivedBy("new"), "the sink of the failed update never gets an event")
	assert.Equal(t, []string{"new"}, reg.stoppedNames(), "the sink built for the failed update is stopped")
}

// The swapped stream keeps the source's lifecycle: when the source crashes,
// the stream is restarted with the stages and sinks of the update.
func TestHotSwappedStreamIsRestartedWithTheNewSinks(t *testing.T) {
	t.Parallel()

	c, reg := newCore(t)
	c.SetRestartDelays(10*time.Millisecond, 50*time.Millisecond)
	_, err := c.UpsertStream(t.Context(), streamSpec(1, sink("old", "fake")))
	require.NoError(t, err)
	_, err = c.UpsertStream(t.Context(), streamSpec(1, sink("new", "fake")))
	require.NoError(t, err)

	close(reg.source(0).crash)
	require.Eventually(t, func() bool { return reg.sourceCount() == 2 }, 5*time.Second, 5*time.Millisecond)
	select {
	case <-reg.source(1).started:
	case <-time.After(5 * time.Second):
		t.Fatal("the restarted source did not run")
	}
	require.NoError(t, deliver(t, reg, logs(1)))

	assert.Equal(t, 1, reg.receivedBy("new"), "the restarted stream delivers to the sink of the update")
	assert.Zero(t, reg.receivedBy("old"))
}

func TestDeletingAHotSwappedStreamStopsTheNewComponents(t *testing.T) {
	t.Parallel()

	c, reg := newCore(t)
	_, err := c.UpsertStream(t.Context(), streamSpec(1, sink("old", "fake")))
	require.NoError(t, err)
	_, err = c.UpsertStream(t.Context(), streamSpec(1, sink("new", "fake")))
	require.NoError(t, err)

	require.NoError(t, c.DeleteStream(t.Context(), "stream"))

	assert.Equal(t, []string{"old", "source", "new"}, reg.stoppedNames(),
		"every component is stopped once, the replaced sink at the swap")
}
