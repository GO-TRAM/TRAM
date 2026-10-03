package api_test

import (
	"context"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"codeberg.org/eljoth/tram/internal/adapters/api"
)

// run starts a.Run and returns a channel that receives its result.
func run(ctx context.Context, a *api.API) <-chan error {
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	return done
}

func TestRunFailsWhenPortIsInUse(t *testing.T) {
	t.Parallel()

	var lc net.ListenConfig
	taken, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = taken.Close() })

	a, err := api.New(nil, "127.0.0.1:0", taken.Addr().String(), slog.New(slog.DiscardHandler))
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

	a, err := api.New(nil, "127.0.0.1:0", "127.0.0.1:0", slog.New(slog.DiscardHandler))
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
