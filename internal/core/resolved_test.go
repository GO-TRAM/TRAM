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

const secretValue = "secret-value"

// outSpecs returns the specs the sink "out" was created with.
func (r *registry) outSpecs() []map[string]any {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.sinkSpecs["out"]
}

// referring returns a stream whose sink refers to a secret, and the same
// stream with its value.
func referring(value string) (domain.StreamSpec, domain.StreamSpec) {
	ref := map[string]any{"valueFrom": map[string]any{"secretKeyRef": map[string]any{"name": "s", "key": "k"}}}
	spec := streamSpec(
		1,
		domain.SinkSpec{Name: "out", Type: "fake", From: "source", Spec: map[string]any{"token": ref}},
	)
	resolved := streamSpec(1,
		domain.SinkSpec{Name: "out", Type: "fake", From: "source", Spec: map[string]any{"token": value}})
	return spec, resolved
}

func TestUpsertResolvedStreamBuildsResolvedAndShowsSpec(t *testing.T) {
	t.Parallel()

	c, reg := newCore(t)
	events := make(chan domain.StreamSpecWatchEvent, 1)
	require.NoError(t, c.Watch(t.Context(), events))

	spec, resolved := referring(secretValue)
	upserted, err := c.UpsertResolvedStream(t.Context(), spec, resolved)
	require.NoError(t, err)

	assert.Equal(t, []map[string]any{{"token": secretValue}}, reg.outSpecs(), "components get the values")

	assert.True(t, spec.Equal(*upserted), "the upsert returns the spec")
	got, err := c.GetStream(t.Context(), "stream")
	require.NoError(t, err)
	assert.True(t, spec.Equal(*got), "GetStream returns the spec")
	all, err := c.GetStreams(t.Context())
	require.NoError(t, err)
	require.Len(t, all, 1)
	assert.True(t, spec.Equal(all[0]), "GetStreams returns the spec")
	event := <-events
	assert.True(t, spec.Equal(event.StreamSpec), "watchers like the config sync get the spec")
	assert.NotContains(t, event.StreamSpec.Sinks[0].Spec, secretValue)
}

func TestUpsertResolvedStreamRestartsOnChangedValue(t *testing.T) {
	t.Parallel()

	c, reg := newCore(t)
	spec, resolved := referring("old")
	_, err := c.UpsertResolvedStream(t.Context(), spec, resolved)
	require.NoError(t, err)

	_, resolved = referring("old")
	_, err = c.UpsertResolvedStream(t.Context(), spec, resolved)
	require.NoError(t, err)
	assert.Len(t, reg.outSpecs(), 1, "the same values change nothing")

	_, rotated := referring("new")
	_, err = c.UpsertResolvedStream(t.Context(), spec, rotated)
	require.NoError(t, err)
	assert.Equal(t, []map[string]any{{"token": "old"}, {"token": "new"}}, reg.outSpecs(),
		"a changed value rebuilds the sink, although the spec is the same")
}

func TestUpsertResolvedStreamRequiresTheSameName(t *testing.T) {
	t.Parallel()

	c, _ := newCore(t)
	spec, resolved := referring("v")
	resolved.Name = "other"
	_, err := c.UpsertResolvedStream(t.Context(), spec, resolved)
	require.ErrorIs(t, err, domain.ErrStreamBuildFailed)
}

func TestRestartedStreamKeepsResolvedValues(t *testing.T) {
	t.Parallel()

	c, reg := newCore(t)
	c.SetRestartDelays(10*time.Millisecond, 50*time.Millisecond)
	spec, resolved := referring("value")
	_, err := c.UpsertResolvedStream(t.Context(), spec, resolved)
	require.NoError(t, err)

	close(reg.source(0).crash)
	require.Eventually(t, func() bool { return len(reg.outSpecs()) == 2 }, 5*time.Second, 5*time.Millisecond)
	assert.Equal(t, map[string]any{"token": "value"}, reg.outSpecs()[1])
	got, err := c.GetStream(t.Context(), "stream")
	require.NoError(t, err)
	assert.True(t, spec.Equal(*got))
}
