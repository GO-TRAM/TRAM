// Package natstest starts a NATS server with JetStream in a container for
// benchmarks and integration tests.
package natstest

import (
	"context"
	"testing"

	"github.com/testcontainers/testcontainers-go"
	tcnats "github.com/testcontainers/testcontainers-go/modules/nats"
)

// Image is the NATS server image the benchmarks run against.
const Image = "nats:2.11"

// Start starts a NATS server with JetStream and returns its URL. The container
// is removed when tb finishes. tb is skipped if no container runtime (Docker
// or Podman) is available.
func Start(tb testing.TB) string {
	tb.Helper()
	ctx := context.Background()
	skipWithoutContainerRuntime(ctx, tb)

	// The module's default command also enables debug and trace logging,
	// which would distort measurements.
	container, err := tcnats.Run(ctx, Image, testcontainers.WithCmd("-js"))
	testcontainers.CleanupContainer(tb, container)
	if err != nil {
		tb.Fatalf("start NATS container: %v", err)
	}

	url, err := container.ConnectionString(ctx)
	if err != nil {
		tb.Fatalf("get NATS URL: %v", err)
	}
	return url
}

// skipWithoutContainerRuntime works like testcontainers.SkipIfProviderIsNotHealthy,
// which only accepts a [testing.T] and not benchmarks.
func skipWithoutContainerRuntime(ctx context.Context, tb testing.TB) {
	tb.Helper()
	defer func() {
		// The provider panics when it cannot find a container runtime.
		if r := recover(); r != nil {
			tb.Skipf("no container runtime available: %v", r)
		}
	}()

	provider, err := testcontainers.ProviderDocker.GetProvider()
	if err != nil {
		tb.Skipf("no container runtime available: %v", err)
	}
	if err = provider.Health(ctx); err != nil {
		tb.Skipf("no container runtime available: %v", err)
	}
}
