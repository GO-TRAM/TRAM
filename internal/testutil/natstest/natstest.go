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

// monitoringPort is the port of the NATS server's HTTP monitoring endpoints.
const monitoringPort = "8222/tcp"

// Server is a NATS server in a container.
type Server struct {
	// URL is the client URL, e.g. nats://localhost:32768.
	URL string
	// MonitoringURL is the base URL of the monitoring endpoints such as
	// /varz, e.g. http://localhost:32769.
	MonitoringURL string
}

// Start starts a NATS server with JetStream and returns its URL. The container
// is removed when tb finishes. tb is skipped if no container runtime (Docker
// or Podman) is available.
func Start(tb testing.TB) string {
	tb.Helper()
	return StartServer(tb).URL
}

// StartServer works like Start and also returns the monitoring URL.
func StartServer(tb testing.TB) Server {
	tb.Helper()
	ctx := context.Background()
	skipWithoutContainerRuntime(ctx, tb)

	// The module's default command also enables debug and trace logging,
	// which would distort measurements.
	container, err := tcnats.Run(ctx, Image, testcontainers.WithCmd("-js", "-m", "8222"))
	testcontainers.CleanupContainer(tb, container)
	if err != nil {
		tb.Fatalf("start NATS container: %v", err)
	}

	url, err := container.ConnectionString(ctx)
	if err != nil {
		tb.Fatalf("get NATS URL: %v", err)
	}
	monitoring, err := container.PortEndpoint(ctx, monitoringPort, "http")
	if err != nil {
		tb.Fatalf("get NATS monitoring URL: %v", err)
	}
	return Server{URL: url, MonitoringURL: monitoring}
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
