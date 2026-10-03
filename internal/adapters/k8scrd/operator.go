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
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	ctrl "sigs.k8s.io/controller-runtime"
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

func New(core ports.Core, kubeconfig string, logger *slog.Logger) (*Operator, error) {
	ctrl.SetLogger(logr.FromSlogHandler(logger.Handler()))

	cfg, err := ctrl.GetConfig()

	if kubeconfig != "" {
		cfg, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
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

	logger.Info("Connecting to Kubernetes", slog.String("host", cfg.Host))

	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:         scheme,
		LeaderElection: false,
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

func (o *Operator) Run(ctx context.Context) error {
	o.logger.InfoContext(ctx, "Starting Kubernetes operator")
	err := o.mgr.Start(ctx)
	o.logger.InfoContext(ctx, "Kubernetes operator stopped", slog.Any("error", err))
	return err
}
