// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

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
	watchNamespaces   []string
	streamSelector    string
	engine            string
	enableFileSync    bool
	pprofEndpoint     string
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
	flags.StringSliceVar(&opts.watchNamespaces, "watch-namespace", nil,
		"Namespaces whose Stream resources the operator runs, comma-separated (default all)")
	flags.StringVar(&opts.streamSelector, "stream-selector", "",
		"Label selector for the Stream resources the operator runs, e.g. tram.io/validation=true")
	flags.StringVar(&opts.engine, "engine", "", "Engine whose Stream resources the operator runs")
	flags.StringVar(&opts.pprofEndpoint, "pprof-endpoint", "",
		"Endpoint serving the Go profiles under /debug/pprof/, e.g. localhost:6060 (default disabled)")
	return cmd
}

func runServer(ctx context.Context, logger *slog.Logger, opts serverOptions) (err error) {
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	metricsHandler, shutdownMetrics, err := setupMetrics()
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, shutdownMetrics(context.WithoutCancel(ctx)))
	}()

	sourceFac := source.NewFactory(logger)
	stageFac := stage.NewFactory()
	sinkFac := sink.NewFactory(logger)

	c := core.New(ctx, logger, sourceFac, stageFac, sinkFac)
	defer func() {
		err = errors.Join(err, c.Close())
	}()

	a, err := api.New(c, opts.grpcEndpoint, opts.httpEndpoint, metricsHandler, logger)
	if err != nil {
		return err
	}

	eg, driversCtx := errgroup.WithContext(ctx)

	if opts.pprofEndpoint != "" {
		eg.Go(func() error {
			return runPprof(driversCtx, opts.pprofEndpoint, logger)
		})
	}

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
		k8scrd.SetLogger(logger)
		eg.Go(func() error {
			op, opErr := k8scrd.New(c, k8scrd.Options{
				Kubeconfig:     opts.kubeconfig,
				Namespaces:     opts.watchNamespaces,
				StreamSelector: opts.streamSelector,
				Engine:         opts.engine,
			}, logger)
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
