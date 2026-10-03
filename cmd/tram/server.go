package main

import (
	"context"
	"errors"
	"log/slog"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"

	"codeberg.org/eljoth/tram/internal/adapters/api"
	"codeberg.org/eljoth/tram/internal/adapters/configsync"
	"codeberg.org/eljoth/tram/internal/adapters/k8scrd"
	"codeberg.org/eljoth/tram/internal/adapters/sink"
	"codeberg.org/eljoth/tram/internal/adapters/source"
	"codeberg.org/eljoth/tram/internal/adapters/stage"

	"codeberg.org/eljoth/tram/internal/core"
)

type serverOptions struct {
	grpcEndpoint      string
	httpEndpoint      string
	configDir         string
	enableK8sOperator bool
	kubeconfig        string
	enableFileSync    bool
}

func newServerCmd(logger *slog.Logger) *cobra.Command {
	var opts serverOptions
	cmd := &cobra.Command{
		Use:   "server",
		Short: "Start the gRPC Gateway HTTP server",
		Long:  `Start the gRPC Gateway HTTP server that proxies requests to the gRPC backend.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runServer(cmd.Context(), logger, opts)
		},
	}

	flags := cmd.Flags()
	flags.StringVar(&opts.grpcEndpoint, "grpc-endpoint", "localhost:50051", "gRPC server endpoint")
	flags.StringVar(&opts.httpEndpoint, "http-endpoint", "localhost:8080", "HTTP server endpoint")
	flags.StringVarP(&opts.configDir, "config-directory", "c", "config", "Configuration directory")
	flags.BoolVar(&opts.enableK8sOperator, "enable-k8s-operator", false, "Enable the Kubernetes operator")
	flags.BoolVar(&opts.enableFileSync, "enable-file-sync", false, "Enable the file sync")
	flags.StringVar(&opts.kubeconfig, "kubeconfig", "", "Kubernetes config file")
	return cmd
}

func runServer(ctx context.Context, logger *slog.Logger, opts serverOptions) (err error) {
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// driven
	sourceFac := source.NewFactory(logger)
	stageFac := stage.NewFactory()
	sinkFac := sink.NewFactory(logger)

	// core
	c := core.New(ctx, logger, sourceFac, stageFac, sinkFac)
	defer func() {
		err = errors.Join(err, c.Close())
	}()

	a, err := api.New(c, opts.grpcEndpoint, opts.httpEndpoint, logger)
	if err != nil {
		return err
	}

	// drivers
	// A driver that fails cancels driversCtx, which stops the others, so Tram
	// exits instead of running without it.
	eg, driversCtx := errgroup.WithContext(ctx)

	if opts.enableFileSync {
		eg.Go(func() error {
			cs, syncErr := configsync.New(c, opts.configDir, logger)
			if syncErr != nil {
				return syncErr
			}
			return errors.Join(cs.Run(driversCtx), cs.Close())
		})
	}

	if opts.enableK8sOperator {
		eg.Go(func() error {
			op, opErr := k8scrd.New(c, opts.kubeconfig, logger)
			if opErr != nil {
				return opErr
			}
			return op.Run(driversCtx)
		})
	}

	eg.Go(func() error {
		return a.Run(driversCtx)
	})

	err = eg.Wait()
	logger.InfoContext(ctx, "Shutting down")
	return err
}
