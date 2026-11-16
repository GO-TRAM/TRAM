// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

//go:generate controller-gen object:headerFile="hack/boilerplate.go.txt" paths="./..."
//go:generate controller-gen crd paths="./..." output:crd:dir=config/crd/bases
//go:generate controller-gen rbac:roleName=manager-role paths="./..." output:rbac:dir=config/rbac

package k8scrd

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/metrics/server"

	tramv1 "codeberg.org/eljoth/tram/internal/adapters/k8scrd/api/v1"
	"codeberg.org/eljoth/tram/internal/adapters/k8scrd/internal/controller"
	"codeberg.org/eljoth/tram/internal/ports"
)

type Operator struct {
	mgr    ctrl.Manager
	core   ports.Core
	logger *slog.Logger
}

type Options struct {
	Kubeconfig     string
	Namespaces     []string
	StreamSelector string

	skipNameValidation bool
}

func SetLogger(logger *slog.Logger) {
	ctrl.SetLogger(logr.FromSlogHandler(logger.Handler()))
}

func New(core ports.Core, opts Options, logger *slog.Logger) (*Operator, error) {
	cacheOpts, err := cacheOptions(opts)
	if err != nil {
		return nil, err
	}

	cfg, err := ctrl.GetConfig()

	if opts.Kubeconfig != "" {
		cfg, err = clientcmd.BuildConfigFromFlags("", opts.Kubeconfig)
	}

	if err != nil {
		return nil, fmt.Errorf("get kube config: %w", err)
	}

	scheme := runtime.NewScheme()
	if err = clientgoscheme.AddToScheme(scheme); err != nil {
		return nil, err
	}
	if err = tramv1.AddToScheme(scheme); err != nil {
		return nil, err
	}

	logger.Info("Connecting to Kubernetes", slog.String("host", cfg.Host),
		slog.Any("namespaces", opts.Namespaces), slog.String("stream_selector", opts.StreamSelector))

	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:         scheme,
		Logger:         logr.FromSlogHandler(logger.Handler()),
		Cache:          cacheOpts,
		LeaderElection: false,
		Controller:     config.Controller{SkipNameValidation: &opts.skipNameValidation},
		Metrics: server.Options{
			BindAddress: "0",
		},
	})
	if err != nil {
		return nil, err
	}

	if err = (&controller.StreamReconciler{
		Client:     mgr.GetClient(),
		Scheme:     mgr.GetScheme(),
		Core:       core,
		References: mgr.GetAPIReader(),
	}).SetupWithManager(mgr); err != nil {
		return nil, fmt.Errorf("set up controller: %w", err)
	}

	return &Operator{mgr: mgr, core: core, logger: logger}, nil
}

// cacheOptions limits the cache to the selected Stream resources. The
// reconciler reads through the cache, so a resource that leaves the selection,
// e.g. because its label was removed, is reconciled as deleted and its stream
// stops.
func cacheOptions(opts Options) (cache.Options, error) {
	var cacheOpts cache.Options

	if len(opts.Namespaces) > 0 {
		cacheOpts.DefaultNamespaces = make(map[string]cache.Config, len(opts.Namespaces))
		for _, ns := range opts.Namespaces {
			// An empty name would stand for all namespaces and silently undo the
			// limit, e.g. after a trailing comma.
			if ns == "" {
				return cache.Options{}, ErrEmptyNamespace
			}
			cacheOpts.DefaultNamespaces[ns] = cache.Config{}
		}
	}

	if opts.StreamSelector != "" {
		selector, err := labels.Parse(opts.StreamSelector)
		if err != nil {
			return cache.Options{}, fmt.Errorf("%w: %w", ErrInvalidStreamSelector, err)
		}
		cacheOpts.DefaultLabelSelector = selector
		// The selector chooses Stream resources, not the secrets and config
		// maps they refer to, whose metadata is watched to notice changes.
		cacheOpts.ByObject = map[client.Object]cache.ByObject{
			&metav1.PartialObjectMetadata{APIVersion: "v1", Kind: "Secret"}: {
				Label: labels.Everything(),
			},
			&metav1.PartialObjectMetadata{APIVersion: "v1", Kind: "ConfigMap"}: {
				Label: labels.Everything(),
			},
		}
	}

	return cacheOpts, nil
}

func (o *Operator) Run(ctx context.Context) error {
	o.logger.InfoContext(ctx, "Starting Kubernetes operator")
	err := o.mgr.Start(ctx)
	o.logger.InfoContext(ctx, "Kubernetes operator stopped", slog.Any("error", err))
	return err
}
