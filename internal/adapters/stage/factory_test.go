// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package stage_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"codeberg.org/eljoth/tram/internal/adapters/stage"
	"codeberg.org/eljoth/tram/internal/core/domain"
)

func TestFactoryUnknownType(t *testing.T) {
	t.Parallel()

	_, err := stage.NewFactory().New(domain.StageSpec{Name: "s", Type: "unknown"})
	require.ErrorIs(t, err, domain.ErrInvalidStageType)
	assert.ErrorContains(t, err, `"unknown"`)
}
