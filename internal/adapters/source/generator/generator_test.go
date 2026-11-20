// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package generator_test

import (
	"testing"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/stretchr/testify/require"

	"codeberg.org/tram/tram/internal/adapters/source/generator"
	"codeberg.org/tram/tram/internal/core/domain"
)

func TestRunReturnsAfterStop(t *testing.T) {
	t.Parallel()

	g, err := generator.New(domain.SourceSpec{
		Name: "generator",
		Spec: map[string]any{"interval": "1h"},
	}, validator.New())
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() { done <- g.Run(t.Context()) }()

	require.NoError(t, g.Stop())

	select {
	case runErr := <-done:
		require.NoError(t, runErr)
	case <-time.After(time.Second):
		t.Fatal("Run did not return after Stop")
	}

	require.NoError(t, g.Stop(), "stopping again is harmless")
}

func TestNewRejectsNonPositiveInterval(t *testing.T) {
	t.Parallel()

	for _, interval := range []string{"0s", "-1s"} {
		_, err := generator.New(domain.SourceSpec{
			Name: "generator",
			Spec: map[string]any{"interval": interval},
		}, validator.New())
		require.ErrorContains(t, err, "not positive", interval)
	}
}
