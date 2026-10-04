//go:generate controller-gen object:headerFile="hack/boilerplate.go.txt" paths="./..."
//go:generate controller-gen crd paths="./..." output:crd:dir=config/crd/bases

// Package k8scrd runs Tram's Kubernetes operator, which turns Stream custom
// resources into streams of the core.
package k8scrd

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
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

// Options selects the cluster and the Stream resources the operator runs.
type Options struct {
	// Kubeconfig is the path of a kubeconfig file. Empty uses the in-cluster
	// configuration or the default kubeconfig.
	Kubeconfig string
	// Namespaces limits the operator to Stream resources in these namespaces.
	// Empty watches all namespaces.
	Namespaces []string
	// StreamSelector is a label selector, e.g. "tram.io/validation=true".
	// Empty runs Stream resources regardless of their labels.
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
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
		Core:   core,
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
	}

	return cacheOpts, nil
}

func (o *Operator) Run(ctx context.Context) error {
	o.logger.InfoContext(ctx, "Starting Kubernetes operator")
	err := o.mgr.Start(ctx)
	o.logger.InfoContext(ctx, "Kubernetes operator stopped", slog.Any("error", err))
	return err
}
