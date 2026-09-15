//go:generate controller-gen object:headerFile="hack/boilerplate.go.txt" paths="./..."
//go:generate controller-gen crd paths="./..." output:crd:dir=config/crd/bases

// internal/adapters/k8scrd/operator.go
package k8scrd

import (
	"context"
	"fmt"
	"log"
	stdlog "log"
	"log/slog"

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
	mgr  ctrl.Manager
	core ports.Core
}

func New(core ports.Core, kubeconfig string) (*Operator, error) {
	// Set slog as the logger for controller-runtime
	ctlrlog.SetLogger(&slogLogger{logger: slog.Default()})

	cfg, err := ctrl.GetConfig()

	if kubeconfig != "" {
		cfg, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
	}

	if err != nil {
		return nil, fmt.Errorf("get kube config: %w", err)
	}

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		return nil, err
	}
	if err := tramv1.AddToScheme(scheme); err != nil {
		return nil, err
	}

	stdlog.Printf("Kube host = %q", cfg.Host)

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

	if err := (&controller.StreamReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
		Core:   core,
	}).SetupWithManager(mgr); err != nil {
		log.Printf("ERROR setting up controller: %v", err)
		return nil, err
	}

	stdlog.Println("Controller setup complete, waiting for cache sync...")

	return &Operator{mgr: mgr, core: core}, nil
}

func (o *Operator) Run(ctx context.Context) error {
	stdlog.Println("Starting controller-manager...")
	err := o.mgr.Start(ctx)
	stdlog.Printf("Controller-manager stopped: %v", err)
	return err
}
