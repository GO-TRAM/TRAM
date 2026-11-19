// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

// Package containertest skips tests that need a container runtime when there
// is none.
package containertest

import (
	"context"
	"testing"

	"github.com/testcontainers/testcontainers-go"
)

const noRuntimeFormat = "no container runtime available: %v"

// SkipWithoutRuntime skips tb if neither Docker nor Podman is reachable.
func SkipWithoutRuntime(ctx context.Context, tb testing.TB) {
	tb.Helper()
	defer func() {
		if r := recover(); r != nil {
			tb.Skipf(noRuntimeFormat, r)
		}
	}()

	provider, err := testcontainers.ProviderDocker.GetProvider()
	if err != nil {
		tb.Skipf(noRuntimeFormat, err)
	}
	if err = provider.Health(ctx); err != nil {
		tb.Skipf(noRuntimeFormat, err)
	}
}
