// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package api_test

import (
	"errors"
	"fmt"
	"testing"

	"codeberg.org/tram/tram/internal/adapters/api"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"codeberg.org/tram/tram/internal/core/domain"
)

func TestToStatus(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		err      error
		expected codes.Code
	}{
		{name: "not found", err: domain.ErrStreamNotFound, expected: codes.NotFound},
		{
			name:     "build failed",
			err:      errors.Join(domain.ErrStreamBuildFailed, domain.ErrGraphNotTree),
			expected: codes.InvalidArgument,
		},
		{name: "core closed", err: fmt.Errorf("upsert: %w", domain.ErrCoreClosed), expected: codes.Unavailable},
		{name: "unknown", err: errors.New("boom"), expected: codes.Internal},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, c.expected, status.Code(api.ToStatus(c.err)))
		})
	}
}
