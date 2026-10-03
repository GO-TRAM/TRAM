package api

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"codeberg.org/eljoth/tram/internal/ports"
	v1 "codeberg.org/eljoth/tram/pkg/gen/go/api/proto/stream/v1"
)

const (
	shutdownTimeout   = 5 * time.Second
	readHeaderTimeout = 10 * time.Second

	// servers is the number of servers Run runs: gRPC and the HTTP gateway.
	servers = 2
)

type API struct {
	grpcServerEndpoint string
	httpServerEndpoint string
	grpcServer         *grpc.Server
	core               ports.Core
	logger             *slog.Logger
}

func New(core ports.Core, grpcServerEndpoint, httpServerEndpoint string, logger *slog.Logger) (*API, error) {
	return &API{
		grpcServerEndpoint: grpcServerEndpoint,
		httpServerEndpoint: httpServerEndpoint,
		core:               core,
		logger:             logger,
	}, nil
}

// Run serves the gRPC API and its HTTP gateway until ctx is done. It returns
// an error if a server cannot listen or stops on its own, so the caller does
// not keep running without the API.
func (a *API) Run(ctx context.Context) error {
	a.grpcServer = grpc.NewServer()

	v1.RegisterStreamServiceServer(a.grpcServer, &streamServer{core: a.core, logger: a.logger})

	// The gateway connects to the gRPC server lazily, so it can be set up
	// before the server listens.
	mux := runtime.NewServeMux()
	opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	if err := v1.RegisterStreamServiceHandlerFromEndpoint(ctx, mux, a.grpcServerEndpoint, opts); err != nil {
		return fmt.Errorf("failed to register gateway handler: %w", err)
	}

	// Both ports are bound before serving, so a port in use fails Run right
	// away.
	var lc net.ListenConfig
	grpcListener, err := lc.Listen(ctx, "tcp", a.grpcServerEndpoint)
	if err != nil {
		return fmt.Errorf("gRPC server failed to listen: %w", err)
	}
	httpListener, err := lc.Listen(ctx, "tcp", a.httpServerEndpoint)
	if err != nil {
		return errors.Join(fmt.Errorf("HTTP gateway failed to listen: %w", err), grpcListener.Close())
	}

	httpServer := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: readHeaderTimeout,
	}

	// serveErrs receives the error of a server that stopped on its own. It
	// holds one error per server, so neither blocks once Run stops reading.
	serveErrs := make(chan error, servers)
	var wg sync.WaitGroup

	wg.Go(func() {
		a.logger.InfoContext(ctx, "gRPC server listening", slog.String("address", a.grpcServerEndpoint))
		if serveErr := a.grpcServer.Serve(grpcListener); serveErr != nil &&
			!errors.Is(serveErr, grpc.ErrServerStopped) {
			serveErrs <- fmt.Errorf("gRPC server failed: %w", serveErr)
		}
	})

	wg.Go(func() {
		a.logger.InfoContext(ctx, "HTTP gateway listening", slog.String("address", a.httpServerEndpoint))
		if serveErr := httpServer.Serve(httpListener); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			serveErrs <- fmt.Errorf("HTTP gateway failed: %w", serveErr)
		}
	})

	var runErr error
	select {
	case <-ctx.Done():
	case runErr = <-serveErrs:
	}

	a.logger.InfoContext(ctx, "Shutting down API servers")

	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
	defer cancel()

	if err = httpServer.Shutdown(shutdownCtx); err != nil {
		a.logger.ErrorContext(ctx, "Could not shut down HTTP gateway cleanly", slog.Any("error", err))
	}

	a.grpcServer.Stop()

	wg.Wait()
	a.logger.InfoContext(ctx, "API servers stopped")
	return runErr
}
