// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package natstest

import (
	"context"
	"testing"

	"github.com/testcontainers/testcontainers-go"
	tcnats "github.com/testcontainers/testcontainers-go/modules/nats"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/go-tram/tram/internal/testutil/containertest"
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
	containertest.SkipWithoutRuntime(ctx, tb)

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
