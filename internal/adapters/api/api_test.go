// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package api_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"codeberg.org/eljoth/tram/internal/adapters/api"
	"codeberg.org/eljoth/tram/internal/adapters/sink"
	"codeberg.org/eljoth/tram/internal/adapters/source"
	"codeberg.org/eljoth/tram/internal/adapters/stage"
	"codeberg.org/eljoth/tram/internal/core"
	"codeberg.org/eljoth/tram/internal/core/domain"
)

const anyLocalAddr = "127.0.0.1:0"

func run(ctx context.Context, a *api.API) <-chan error {
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	return done
}

func TestRunFailsWhenPortIsInUse(t *testing.T) {
	t.Parallel()

	var lc net.ListenConfig
	taken, err := lc.Listen(t.Context(), "tcp", anyLocalAddr)
	require.NoError(t, err)
	t.Cleanup(func() { _ = taken.Close() })

	a, err := api.New(nil, anyLocalAddr, taken.Addr().String(), nil, slog.New(slog.DiscardHandler))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := run(ctx, a)

	select {
	case err = <-done:
		require.ErrorContains(t, err, "HTTP gateway failed to listen")
	case <-time.After(5 * time.Second):
		cancel()
		<-done
		t.Fatal("Run kept running although the HTTP port is in use")
	}
}

func TestRunStopsWithContext(t *testing.T) {
	t.Parallel()

	a, err := api.New(nil, anyLocalAddr, anyLocalAddr, nil, slog.New(slog.DiscardHandler))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	done := run(ctx, a)
	cancel()

	select {
	case err = <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not stop after its context was cancelled")
	}
}

func TestListStreams(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.DiscardHandler)
	c := core.New(t.Context(), logger, source.NewFactory(logger), stage.NewFactory(), sink.NewFactory(logger))
	t.Cleanup(func() { assert.NoError(t, c.Close()) })
	for _, name := range []string{"b", "a"} {
		_, err := c.UpsertStream(t.Context(), domain.StreamSpec{
			Name:   name,
			Source: domain.SourceSpec{Name: "in", Type: "generator", Spec: map[string]any{"interval": "1h"}},
			Sinks:  []domain.SinkSpec{{Name: "out", Type: "debug", From: "in"}},
		})
		require.NoError(t, err)
	}

	// The gateway dials the gRPC endpoint by its address, so it needs a port
	// known in advance.
	httpAddr, grpcAddr := freeAddr(t), freeAddr(t)
	a, err := api.New(c, grpcAddr, httpAddr, nil, logger)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	done := run(ctx, a)
	t.Cleanup(func() {
		cancel()
		require.NoError(t, <-done)
	})

	var body struct {
		Streams []struct {
			Name string `json:"name"`
		} `json:"streams"`
	}
	require.Eventually(t, func() bool {
		req, reqErr := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+httpAddr+"/v1/streams", nil)
		require.NoError(t, reqErr)
		resp, getErr := http.DefaultClient.Do(req)
		if getErr != nil {
			return false
		}
		defer resp.Body.Close()
		return resp.StatusCode == http.StatusOK && json.NewDecoder(resp.Body).Decode(&body) == nil
	}, 5*time.Second, 20*time.Millisecond)

	require.Len(t, body.Streams, 2)
	assert.Equal(t, "a", body.Streams[0].Name, "ordered by name")
	assert.Equal(t, "b", body.Streams[1].Name)
}

func TestMetricsEndpoint(t *testing.T) {
	t.Parallel()

	metrics := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("tram_metric 1\n"))
	})
	httpAddr, grpcAddr := freeAddr(t), freeAddr(t)
	a, err := api.New(nil, grpcAddr, httpAddr, metrics, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	done := run(ctx, a)
	t.Cleanup(func() {
		cancel()
		require.NoError(t, <-done)
	})

	var body []byte
	require.Eventually(t, func() bool {
		req, reqErr := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+httpAddr+"/metrics", nil)
		require.NoError(t, reqErr)
		resp, getErr := http.DefaultClient.Do(req)
		if getErr != nil {
			return false
		}
		defer resp.Body.Close()
		body, getErr = io.ReadAll(resp.Body)
		return resp.StatusCode == http.StatusOK && getErr == nil
	}, 5*time.Second, 20*time.Millisecond)

	assert.Equal(t, "tram_metric 1\n", string(body))
}

func freeAddr(t *testing.T) string {
	t.Helper()
	var lc net.ListenConfig
	l, err := lc.Listen(t.Context(), "tcp", anyLocalAddr)
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())
	return addr
}
