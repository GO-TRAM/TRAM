// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package source_test

import (
	"log/slog"

	"codeberg.org/tram/tram/internal/adapters/source"

	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"codeberg.org/tram/tram/internal/core/domain"
)

func TestFactoryUnknownType(t *testing.T) {
	t.Parallel()

	_, err := source.NewFactory(slog.New(slog.DiscardHandler)).New(domain.SourceSpec{Name: "s", Type: "unknown"})
	require.ErrorIs(t, err, domain.ErrInvalidSourceType)
	assert.ErrorContains(t, err, `"unknown"`)
}
