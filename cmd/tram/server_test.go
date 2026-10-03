package main

import (
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A driver that cannot start stops the server, instead of leaving it running
// without the driver until it is signalled.
func TestRunServerStopsWhenDriverFails(t *testing.T) {
	t.Parallel()

	done := make(chan error, 1)
	go func() {
		done <- runServer(t.Context(), slog.New(slog.DiscardHandler), serverOptions{
			grpcEndpoint:   "127.0.0.1:0",
			httpEndpoint:   "127.0.0.1:0",
			configDir:      filepath.Join(t.TempDir(), "missing"),
			enableFileSync: true,
		})
	}()

	select {
	case err := <-done:
		require.ErrorContains(t, err, "failed to watch directory")
	case <-time.After(10 * time.Second):
		t.Fatal("server kept running although the file sync could not start")
	}
}
