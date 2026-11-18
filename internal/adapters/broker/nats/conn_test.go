// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package nats_test

import (
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	natsdriver "codeberg.org/eljoth/tram/internal/adapters/broker/nats"
	"codeberg.org/eljoth/tram/internal/testutil/natstest"
)

// proxy forwards TCP connections to target and can simulate an outage.
type proxy struct {
	t      *testing.T
	addr   string
	target string

	mu       sync.Mutex
	listener net.Listener
	conns    []net.Conn
}

func newProxy(t *testing.T, target string) *proxy {
	t.Helper()
	p := &proxy{t: t, target: target}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	p.addr = listener.Addr().String()
	p.serve(listener)
	t.Cleanup(p.cut)
	return p
}

func (p *proxy) serve(listener net.Listener) {
	p.mu.Lock()
	p.listener = listener
	p.mu.Unlock()
	go func() {
		for {
			client, err := listener.Accept()
			if err != nil {
				return
			}
			server, err := net.Dial("tcp", p.target)
			if err != nil {
				_ = client.Close()
				continue
			}
			p.mu.Lock()
			p.conns = append(p.conns, client, server)
			p.mu.Unlock()
			go func() { _, _ = io.Copy(server, client); _ = server.Close() }()
			go func() { _, _ = io.Copy(client, server); _ = client.Close() }()
		}
	}()
}

// cut closes all connections and refuses new ones until restore.
func (p *proxy) cut() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.listener != nil {
		_ = p.listener.Close()
		p.listener = nil
	}
	for _, c := range p.conns {
		_ = c.Close()
	}
	p.conns = nil
}

func (p *proxy) restore() {
	p.t.Helper()
	listener, err := net.Listen("tcp", p.addr)
	require.NoError(p.t, err)
	p.serve(listener)
}

func TestConnectionOutlastsDefaultReconnectAttempts(t *testing.T) {
	t.Parallel()

	server := strings.TrimPrefix(natstest.Start(t), "nats://")
	p := newProxy(t, server)

	const wait = 10 * time.Millisecond
	nc, err := natsdriver.ConnectRetryingEvery("nats://"+p.addr, slog.New(slog.DiscardHandler), wait)
	require.NoError(t, err)
	t.Cleanup(nc.Close)

	p.cut()
	require.Eventually(t, func() bool { return !nc.IsConnected() }, 5*time.Second, wait)

	// Publishing while disconnected fails instead of being buffered.
	require.ErrorIs(t, nc.Publish("test", []byte("x")), nats.ErrReconnectBufExceeded)

	// Well beyond nats.go's default of 60 attempts.
	time.Sleep(100 * wait)
	assert.False(t, nc.IsClosed(), "the connection keeps reconnecting")

	p.restore()
	require.Eventually(t, nc.IsConnected, 10*time.Second, wait, "the connection is back")
	require.NoError(t, nc.Publish("test", []byte("x")))
	require.NoError(t, nc.Flush())
}
