package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"codeberg.org/eljoth/tram/internal/adapters/api"
	"codeberg.org/eljoth/tram/internal/adapters/configsync"
	"codeberg.org/eljoth/tram/internal/adapters/k8scrd"
	"codeberg.org/eljoth/tram/internal/adapters/sink"
	"codeberg.org/eljoth/tram/internal/adapters/source"
	"codeberg.org/eljoth/tram/internal/adapters/stage"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"

	"codeberg.org/eljoth/tram/internal/core"
)

var (
	grpcEndpoint      string
	httpEndpoint      string
	configDir         string
	enableK8sOperator bool
	kubeconfig        string
	enableFileSync    bool
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

	// driven
	sourceFac := source.NewFactory()
	stageFac := stage.NewFactory()
	sinkFac := sink.NewFactory()

	// core
	c := core.New(
		ctx,
		sourceFac,
		stageFac,
		sinkFac,
	)
	defer func() {
		if err := c.Close(); err != nil {
			log.Printf("Failed to close core: %v", err)
		}
	}()

	// driver

	eg := errgroup.Group{}

	if enableFileSync {
		eg.Go(func() error {
			cs, err := configsync.New(c, configDir)
			if err != nil {
				return err
			}
			defer cs.Close()
			return cs.Run(ctx)
		})
	}

	if enableK8sOperator {
		eg.Go(func() error {
			op, err := k8scrd.New(c, kubeconfig)
			if err != nil {
				return err
			}
			return op.Run(ctx)
		})
	}

	a, err := api.New(c, grpcEndpoint, httpEndpoint)
	if err != nil {
		log.Fatal(err)
	}

	eg.Go(func() error {
		return a.Run(ctx)
	})

	if err := eg.Wait(); err != nil {
		log.Printf("Failed to run server: %v", err)
	}
}

func init() {
	rootCmd.AddCommand(serverCmd)

	serverCmd.Flags().StringVar(&grpcEndpoint, "grpc-endpoint", "localhost:50051", "gRPC server endpoint")
	serverCmd.Flags().StringVar(&httpEndpoint, "http-endpoint", "localhost:8080", "HTTP server endpoint")
	serverCmd.Flags().StringVarP(&configDir, "config-directory", "c", "config", "Configuration directory")
	serverCmd.Flags().BoolVar(&enableK8sOperator, "enable-k8s-operator", false, "Enable the Kubernetes operator")
	serverCmd.Flags().BoolVar(&enableFileSync, "enable-file-sync", false, "Enable the file sync")
	serverCmd.Flags().StringVar(&kubeconfig, "kubeconfig", "", "Kubernetes config file")
}
