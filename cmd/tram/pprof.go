// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"runtime"
	"time"
)

const (
	pprofReadHeaderTimeout = 5 * time.Second
	pprofShutdownTimeout   = 5 * time.Second

	// Record one in this many mutex contention events and blocking events of
	// at least this many nanoseconds, so the mutex and block profiles are not
	// empty. Both cost a little on every contended lock, which is why they are
	// only enabled together with the endpoint.
	pprofMutexProfileFraction = 100
	pprofBlockProfileRate     = int(10 * time.Millisecond)
)

// runPprof serves the Go profiles on their own endpoint until ctx ends. They
// have no authentication, so they must not share the public HTTP endpoint.
func runPprof(ctx context.Context, endpoint string, logger *slog.Logger) error {
	listener, err := new(net.ListenConfig).Listen(ctx, "tcp", endpoint)
	if err != nil {
		return fmt.Errorf("failed to listen for pprof on %s: %w", endpoint, err)
	}
	return servePprof(ctx, listener, logger)
}

func servePprof(ctx context.Context, listener net.Listener, logger *slog.Logger) error {
	runtime.SetMutexProfileFraction(pprofMutexProfileFraction)
	runtime.SetBlockProfileRate(pprofBlockProfileRate)

	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	// No write timeout: CPU profiles and traces stream for as long as asked.
	server := &http.Server{Handler: mux, ReadHeaderTimeout: pprofReadHeaderTimeout}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), pprofShutdownTimeout)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()

	logger.InfoContext(ctx, "Serving pprof", slog.String("endpoint", listener.Addr().String()))
	if err := server.Serve(listener); !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("pprof server failed: %w", err)
	}
	return nil
}
