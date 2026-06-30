package api

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"codeberg.org/eljoth/stream/internal/ports"
	v1 "codeberg.org/eljoth/stream/pkg/gen/go/api/proto/stream/v1"
)

type API struct {
	grpcServerEndpoint string
	httpServerEndpoint string
	grpcServer         *grpc.Server
	core               ports.Core
}

func New(core ports.Core, grpcServerEndpoint, httpServerEndpoint string) *API {
	return &API{
		grpcServerEndpoint: grpcServerEndpoint,
		httpServerEndpoint: httpServerEndpoint,
	}
}

func (a *API) Run(ctx context.Context) error {
	a.grpcServer = grpc.NewServer()

	v1.RegisterStreamServiceServer(a.grpcServer, &streamServer{core: a.core})

	grpcListener, err := net.Listen("tcp", a.grpcServerEndpoint)
	if err != nil {
		return fmt.Errorf("gRPC server failed to listen: %v", err)
	}

	var wg sync.WaitGroup

	wg.Go(func() {
		log.Printf("gRPC server listening on %s\n", a.grpcServerEndpoint)
		if err := a.grpcServer.Serve(grpcListener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			log.Printf("gRPC server failed: %v\n", err)
		}
	})

	mux := runtime.NewServeMux()
	opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}

	err = v1.RegisterStreamServiceHandlerFromEndpoint(ctx, mux, a.grpcServerEndpoint, opts)
	if err != nil {
		return fmt.Errorf("failed to register gateway handler: %v", err)
	}

	httpServer := &http.Server{
		Addr:    a.httpServerEndpoint,
		Handler: mux,
	}

	wg.Go(func() {
		log.Printf("HTTP Gateway listening on %s\n", a.httpServerEndpoint)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("HTTP gateway failed: %v\n", err)
		}
	})

	<-ctx.Done()

	log.Println("Shutting down servers...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP server shutdown error: %v\n", err)
	}

	a.grpcServer.Stop()

	wg.Wait()
	log.Println("Servers stopped gracefully")
	return nil
}
