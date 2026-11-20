// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package debug_test

import (
	"log/slog"

	"codeberg.org/tram/tram/internal/adapters/sink/debug"

	"testing"

	"github.com/go-playground/validator/v10"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"codeberg.org/tram/tram/internal/core/domain"
)

func TestNotifyWithoutPath(t *testing.T) {
	t.Parallel()

	d, err := debug.New(domain.SinkSpec{Name: "debug"}, validator.New(), slog.New(slog.DiscardHandler))
	require.NoError(t, err)

	assert.NotPanics(t, func() {
		assert.NoError(t, d.Notify(t.Context(), &domain.Event{}))
	})
}
