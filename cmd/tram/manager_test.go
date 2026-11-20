// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"codeberg.org/tram/tram/internal/adapters/k8scrd"
)

// Not parallel: the manager sets controller-runtime's global logger.
//
//nolint:paralleltest // see above
func TestRunManagerRejectsEmptyNamespace(t *testing.T) {
	err := runManager(t.Context(), slog.New(slog.DiscardHandler), managerOptions{
		watchNamespaces: []string{"telemetry", ""},
	})
	require.ErrorIs(t, err, k8scrd.ErrEmptyNamespace)
}

func TestManagerFlagDefaults(t *testing.T) {
	t.Parallel()

	cmd := newManagerCmd(slog.New(slog.DiscardHandler))
	leaderElect, err := cmd.Flags().GetBool("leader-elect")
	require.NoError(t, err)
	assert.True(t, leaderElect, "replicas of the manager elect a leader by default")
}
