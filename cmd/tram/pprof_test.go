// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPprofServesProfilesUntilStopped(t *testing.T) {
	t.Parallel()

	listener, err := new(net.ListenConfig).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	base := "http://" + listener.Addr().String()

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- servePprof(ctx, listener, slog.New(slog.DiscardHandler)) }()

	for _, path := range []string{"/debug/pprof/heap", "/debug/pprof/goroutine?debug=1", "/debug/pprof/mutex"} {
		body := get(t, base+path)
		assert.NotEmpty(t, body, path)
	}
	assert.Contains(t, get(t, base+"/debug/pprof/"), "goroutine", "the index lists the profiles")

	cancel()
	select {
	case err = <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("the pprof server did not stop")
	}
}

func TestRunPprofFailsOnInvalidEndpoint(t *testing.T) {
	t.Parallel()

	err := runPprof(t.Context(), "invalid-endpoint", slog.New(slog.DiscardHandler))
	require.ErrorContains(t, err, "failed to listen for pprof")
}

func get(t *testing.T, url string) string {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode, url)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return string(body)
}
