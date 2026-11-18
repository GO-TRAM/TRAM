// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

// Package tcpproxy forwards TCP connections and simulates outages, for tests
// of reconnecting clients.
package tcpproxy

import (
	"context"
	"io"
	"net"
	"sync"
	"testing"
)

// Proxy forwards connections to its target until Cut.
type Proxy struct {
	tb     testing.TB
	addr   string
	target string

	mu       sync.Mutex
	listener net.Listener
	conns    []net.Conn
}

// New listens on a free local port and forwards to target, host:port.
func New(tb testing.TB, target string) *Proxy {
	tb.Helper()
	p := &Proxy{tb: tb, target: target}
	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("listen: %v", err)
	}
	p.addr = listener.Addr().String()
	p.serve(listener)
	tb.Cleanup(p.Cut)
	return p
}

// Addr is where clients connect, host:port.
func (p *Proxy) Addr() string { return p.addr }

func (p *Proxy) serve(listener net.Listener) {
	p.mu.Lock()
	p.listener = listener
	p.mu.Unlock()
	go func() {
		for {
			client, err := listener.Accept()
			if err != nil {
				return
			}
			server, err := (&net.Dialer{}).DialContext(context.Background(), "tcp", p.target)
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

// Cut closes all connections and refuses new ones until Restore.
func (p *Proxy) Cut() {
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

// Restore accepts connections again on the same address.
func (p *Proxy) Restore() {
	p.tb.Helper()
	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", p.addr)
	if err != nil {
		p.tb.Fatalf("listen again: %v", err)
	}
	p.serve(listener)
}
