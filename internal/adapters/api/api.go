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

func (a *API) Run(ctx context.Context) error {
	a.grpcServer = grpc.NewServer()

	v1.RegisterStreamServiceServer(a.grpcServer, &streamServer{core: a.core, logger: a.logger})

	var lc net.ListenConfig
	grpcListener, err := lc.Listen(ctx, "tcp", a.grpcServerEndpoint)
	if err != nil {
		return fmt.Errorf("gRPC server failed to listen: %w", err)
	}

	var wg sync.WaitGroup

	wg.Go(func() {
		a.logger.InfoContext(ctx, "gRPC server listening", slog.String("address", a.grpcServerEndpoint))
		if serveErr := a.grpcServer.Serve(
			grpcListener,
		); serveErr != nil &&
			!errors.Is(serveErr, grpc.ErrServerStopped) {
			a.logger.ErrorContext(ctx, "gRPC server failed", slog.Any("error", serveErr))
		}
	})

	mux := runtime.NewServeMux()
	opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}

	err = v1.RegisterStreamServiceHandlerFromEndpoint(ctx, mux, a.grpcServerEndpoint, opts)
	if err != nil {
		return fmt.Errorf("failed to register gateway handler: %w", err)
	}

	httpServer := &http.Server{
		Addr:              a.httpServerEndpoint,
		Handler:           mux,
		ReadHeaderTimeout: readHeaderTimeout,
	}

	wg.Go(func() {
		a.logger.InfoContext(ctx, "HTTP gateway listening", slog.String("address", a.httpServerEndpoint))
		if serveErr := httpServer.ListenAndServe(); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			a.logger.ErrorContext(ctx, "HTTP gateway failed", slog.Any("error", serveErr))
		}
	})

	<-ctx.Done()

	a.logger.InfoContext(ctx, "Shutting down API servers")

	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
	defer cancel()

	if err = httpServer.Shutdown(shutdownCtx); err != nil {
		a.logger.ErrorContext(ctx, "Could not shut down HTTP gateway cleanly", slog.Any("error", err))
	}

	a.grpcServer.Stop()

	wg.Wait()
	a.logger.InfoContext(ctx, "API servers stopped")
	return nil
}
