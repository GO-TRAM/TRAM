// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package nats_test

import (
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	natsdriver "github.com/go-tram/tram/internal/adapters/broker/nats"
	"github.com/go-tram/tram/internal/testutil/natstest"
	"github.com/go-tram/tram/internal/testutil/tcpproxy"
)

func TestConnectionOutlastsDefaultReconnectAttempts(t *testing.T) {
	t.Parallel()

	server := strings.TrimPrefix(natstest.Start(t), "nats://")
	p := tcpproxy.New(t, server)

	const wait = 10 * time.Millisecond
	nc, err := natsdriver.ConnectRetryingEvery("nats://"+p.Addr(), slog.New(slog.DiscardHandler), wait)
	require.NoError(t, err)
	t.Cleanup(nc.Close)

	p.Cut()
	require.Eventually(t, func() bool { return !nc.IsConnected() }, 5*time.Second, wait)

	// Publishing while disconnected fails instead of being buffered.
	require.ErrorIs(t, nc.Publish("test", []byte("x")), nats.ErrReconnectBufExceeded)

	// Well beyond nats.go's default of 60 attempts.
	time.Sleep(100 * wait)
	assert.False(t, nc.IsClosed(), "the connection keeps reconnecting")

	p.Restore()
	require.Eventually(t, nc.IsConnected, 10*time.Second, wait, "the connection is back")
	require.NoError(t, nc.Publish("test", []byte("x")))
	require.NoError(t, nc.Flush())
}
