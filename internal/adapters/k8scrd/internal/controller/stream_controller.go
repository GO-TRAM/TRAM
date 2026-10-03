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
	"encoding/json"
	"errors"
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	tramv1 "codeberg.org/eljoth/tram/internal/adapters/k8scrd/api/v1"
	"codeberg.org/eljoth/tram/internal/core/domain"
	"codeberg.org/eljoth/tram/internal/ports"
)

// StreamReconciler reconciles a Stream object.
type StreamReconciler struct {
	client.Client

	Core   ports.Core
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=io.tram.core,resources=streams,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=io.tram.core,resources=streams/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=io.tram.core,resources=streams/finalizers,verbs=update

// Reconcile makes the core run the stream described by a Stream resource, or
// removes it from the core once the resource is gone.
func (r *StreamReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	name := StreamName(req.NamespacedName)

	var stream tramv1.Stream
	if err := r.Get(ctx, req.NamespacedName, &stream); err != nil {
		if client.IgnoreNotFound(err) != nil {
			return ctrl.Result{}, fmt.Errorf("get stream: %w", err)
		}
		if deleteErr := r.Core.DeleteStream(ctx, name); deleteErr != nil &&
			!errors.Is(deleteErr, domain.ErrStreamNotFound) {
			return ctrl.Result{}, fmt.Errorf("delete from core: %w", deleteErr)
		}
		log.Info("Deleted Stream from core", "stream", name)
		return ctrl.Result{}, nil
	}

	desired, err := toDomain(name, stream)
	if err != nil {
		// Retrying cannot fix a malformed spec; the next edit triggers a new reconcile.
		return ctrl.Result{}, errors.Join(r.setActive(ctx, &stream, false), reconcile.TerminalError(err))
	}

	current, err := r.Core.GetStream(ctx, name)
	if err != nil && !errors.Is(err, domain.ErrStreamNotFound) {
		return ctrl.Result{}, fmt.Errorf("get from core: %w", err)
	}

	// Upserting restarts the stream, so only do it when the spec changed.
	if current == nil || !current.Equal(*desired) {
		if _, err = r.Core.UpsertStream(ctx, *desired); err != nil {
			return ctrl.Result{}, errors.Join(r.setActive(ctx, &stream, false), fmt.Errorf("upsert into core: %w", err))
		}
		log.Info("Upserted Stream into core", "stream", name, "generation", stream.Generation)
	}

	return ctrl.Result{}, r.setActive(ctx, &stream, true)
}

// setActive updates the status only when it changes, so reconciles do not
// produce needless writes.
func (r *StreamReconciler) setActive(ctx context.Context, stream *tramv1.Stream, active bool) error {
	if stream.Status.Active == active {
		return nil
	}
	stream.Status.Active = active
	if err := r.Status().Update(ctx, stream); err != nil {
		return fmt.Errorf("update status: %w", err)
	}
	return nil
}

// StreamName returns the core stream name for a Stream resource. Namespaces
// cannot contain dots, so the first dot separates namespace and name.
func StreamName(nn types.NamespacedName) string {
	return nn.Namespace + "." + nn.Name
}

func toDomain(name string, s tramv1.Stream) (*domain.StreamSpec, error) {
	sourceSpec, err := specToMap(s.Spec.Source.Spec)
	if err != nil {
		return nil, fmt.Errorf("source %s: %w", s.Spec.Source.Name, err)
	}

	// Map source (single)
	source := domain.SourceSpec{
		Name: s.Spec.Source.Name,
		Type: s.Spec.Source.Type,
		Spec: sourceSpec,
	}

	// Map stages (list)
	var stages []domain.StageSpec
	for _, stage := range s.Spec.Stages {
		spec, specErr := specToMap(stage.Spec)
		if specErr != nil {
			return nil, fmt.Errorf("stage %s: %w", stage.Name, specErr)
		}
		stages = append(stages, domain.StageSpec{
			Name: stage.Name,
			Type: stage.Type,
			From: stage.From,
			Spec: spec,
		})
	}

	// Map sinks (list)
	var sinks []domain.SinkSpec
	for _, sink := range s.Spec.Sinks {
		spec, specErr := specToMap(sink.Spec)
		if specErr != nil {
			return nil, fmt.Errorf("sink %s: %w", sink.Name, specErr)
		}
		sinks = append(sinks, domain.SinkSpec{
			Name: sink.Name,
			Type: sink.Type,
			From: sink.From,
			Spec: spec,
		})
	}

	return &domain.StreamSpec{
		Name:   name,
		Source: source,
		Stages: stages,
		Sinks:  sinks,
	}, nil
}

// specToMap decodes a component spec the same way the API and file sync do,
// so all drivers hand identical values to the core.
func specToMap(raw *runtime.RawExtension) (map[string]any, error) {
	var spec map[string]any
	if raw == nil || len(raw.Raw) == 0 {
		return spec, nil
	}
	if err := json.Unmarshal(raw.Raw, &spec); err != nil {
		return nil, fmt.Errorf("decode spec: %w", err)
	}
	return spec, nil
}

func (r *StreamReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		// Status writes and periodic resyncs do not change the generation, so
		// they no longer trigger a reconcile.
		For(&tramv1.Stream{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Named("stream").
		Complete(r)
}
