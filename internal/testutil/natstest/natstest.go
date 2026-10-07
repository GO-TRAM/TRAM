package natstest

import (
	"context"
	"testing"

	"github.com/testcontainers/testcontainers-go"
	tcnats "github.com/testcontainers/testcontainers-go/modules/nats"
	"github.com/testcontainers/testcontainers-go/wait"
)

const Image = "nats:2.11"

const monitoringPort = "8222/tcp"

type Server struct {
	URL           string
	MonitoringURL string
}

func Start(tb testing.TB) string {
	tb.Helper()
	return StartServer(tb).URL
}

func StartServer(tb testing.TB) Server {
	tb.Helper()
	ctx := context.Background()
	skipWithoutContainerRuntime(ctx, tb)


	container, err := tcnats.Run(ctx, Image,
		testcontainers.WithCmd("-js", "-m", "8222"),
		testcontainers.WithAdditionalWaitStrategy(wait.ForLog("Server is ready")))
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

func skipWithoutContainerRuntime(ctx context.Context, tb testing.TB) {
	tb.Helper()
	defer func() {
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
