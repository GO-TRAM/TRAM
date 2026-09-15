/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"fmt"
	"log"

	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tramv1 "codeberg.org/eljoth/tram/internal/adapters/k8scrd/api/v1"
	"codeberg.org/eljoth/tram/internal/core/domain"
	"codeberg.org/eljoth/tram/internal/ports"
)

// StreamReconciler reconciles a Stream object
type StreamReconciler struct {
	client.Client
	Core   ports.Core
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=io.tram.core,resources=streams,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=io.tram.core,resources=streams/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=io.tram.core,resources=streams/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the Stream object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.24.1/pkg/reconcile
func (r *StreamReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {

	var stream tramv1.Stream
	if err := r.Get(ctx, req.NamespacedName, &stream); err != nil {
		if client.IgnoreNotFound(err) != nil {
			return ctrl.Result{}, fmt.Errorf("get stream: %w", err)
		}
		log.Printf("Stream %s/%s deleted, deleting from core", req.Namespace, req.Name)
		if err := r.Core.DeleteStream(ctx, req.Name); err != nil {
			return ctrl.Result{}, fmt.Errorf("delete from core: %w", err)
		}
		return ctrl.Result{}, nil
	}

	domainSpec := toDomain(stream.Spec)
	_, err := r.Core.UpsertStream(ctx, *domainSpec)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("upsert into core: %w", err)
	}

	stream.Status.Active = true
	if err := r.Status().Update(ctx, &stream); err != nil {
		return ctrl.Result{}, err
	}

	log.Printf("Reconciled %s/%s (version: %s)", req.Namespace, req.Name, stream.ResourceVersion)

	return ctrl.Result{}, nil
}

func toDomain(s tramv1.StreamSpec) *domain.StreamSpec {
	// Convert map[string]string to map[string]any
	convertSpec := func(m map[string]string) map[string]any {
		result := make(map[string]any, len(m))
		for k, v := range m {
			result[k] = v
		}
		return result
	}

	// Map source (single)
	sourceSpec := domain.SourceSpec{
		Name: s.Source.Name,
		Type: s.Source.Type,
		Spec: convertSpec(s.Source.Spec),
	}

	// Map stages (list)
	var stageSpecs []domain.StageSpec
	for _, stage := range s.Stages {
		stageSpecs = append(stageSpecs, domain.StageSpec{
			Name: stage.Name,
			Type: stage.Type,
			From: stage.From,
			Spec: convertSpec(stage.Spec),
		})
	}

	// Map sinks (list)
	var sinkSpecs []domain.SinkSpec
	for _, sink := range s.Sinks {
		sinkSpecs = append(sinkSpecs, domain.SinkSpec{
			Name: sink.Name,
			Type: sink.Type,
			From: sink.From,
			Spec: convertSpec(sink.Spec),
		})
	}

	return &domain.StreamSpec{
		Name:   s.Name,
		Source: sourceSpec,
		Stages: stageSpecs,
		Sinks:  sinkSpecs,
	}
}

func (r *StreamReconciler) SetupWithManager(mgr ctrl.Manager) error {
	log.Println("Setting up Stream controller...")
	return ctrl.NewControllerManagedBy(mgr).
		For(&tramv1.Stream{}).
		Named("stream").
		Complete(r)
}
