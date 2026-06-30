package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"codeberg.org/eljoth/stream/internal/adapters/api"
	"github.com/spf13/cobra"

	"codeberg.org/eljoth/stream/internal/core"
)

var (
	grpcEndpoint string
	httpEndpoint string
)

var serverCmd = &cobra.Command{
	Use:   "server",
	Short: "Start the gRPC Gateway HTTP server",
	Long:  `Start the gRPC Gateway HTTP server that proxies requests to the gRPC backend.`,
	Run: func(cmd *cobra.Command, args []string) {
		runServer(cmd.Context())
	},
}

func runServer(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Println("Shutting down...")
		cancel()
	}()

	c := core.New()
	a := api.New(c, grpcEndpoint, httpEndpoint)

	log.Printf("Starting gRPC Gateway server on %s, forwarding to gRPC at %s", httpEndpoint, grpcEndpoint)
	if err := a.Run(ctx); err != nil {
		log.Fatalf("Failed to run server: %v", err)
	}
}

func init() {
	rootCmd.AddCommand(serverCmd)

	serverCmd.Flags().StringVar(&grpcEndpoint, "grpc-endpoint", "localhost:50051", "gRPC server endpoint")
	serverCmd.Flags().StringVar(&httpEndpoint, "http-endpoint", "localhost:8080", "HTTP server endpoint")
}
