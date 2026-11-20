// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

// Package engines holds the controllers of `tram manager`: it runs every
// Engine as a Deployment of Tram instances, and assigns Streams to Engines by
// labels derived from their spec.engineRefs, which the instances select. It
// runs no streams itself.
package engines

import (
	"context"
	"fmt"
	"maps"

	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	tramv1 "github.com/go-tram/tram/internal/adapters/k8scrd/api/v1"
	tramv1alpha1 "github.com/go-tram/tram/internal/adapters/k8scrd/api/v1alpha1"
)

// StreamReconciler keeps the engine.tram.io/<name> labels of every Stream
// equal to its spec.engineRefs, and reports in status.engines whether each
// referenced Engine exists. It owns all labels with that prefix: it adds the
// missing ones and removes those no reference names, including labels set by
// hand.
type StreamReconciler struct {
	client.Client
}

// +kubebuilder:rbac:groups=io.tram.core,resources=streams,verbs=get;list;watch;patch
// +kubebuilder:rbac:groups=io.tram.core,resources=streams/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=coordination.k8s.io,resources=leases,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

func (r *StreamReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var stream tramv1.Stream
	if err := r.Get(ctx, req.NamespacedName, &stream); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !stream.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	if err := r.syncLabels(ctx, &stream); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, r.syncStatus(ctx, &stream)
}

func (r *StreamReconciler) syncLabels(ctx context.Context, stream *tramv1.Stream) error {
	labels := tramv1.WithEngineLabels(stream.Labels, stream.Spec.EngineRefs)
	if maps.Equal(labels, stream.Labels) {
		return nil
	}

	// A merge patch of the labels alone: other labels, e.g. set by a user at
	// the same time, are left alone.
	patch := client.MergeFrom(stream.DeepCopy())
	stream.Labels = labels
	if err := r.Patch(ctx, stream, patch); err != nil {
		return fmt.Errorf("patch engine labels: %w", err)
	}

	logf.FromContext(ctx).Info("Updated engine labels of Stream",
		"stream", client.ObjectKeyFromObject(stream), "engines", engineNames(stream.Spec.EngineRefs))
	return nil
}

// syncStatus writes an entry per engine reference to status.engines, with an
// Accepted condition that tells whether the Engine exists. Only the manager
// writes status.engines; the instances write status.active.
func (r *StreamReconciler) syncStatus(ctx context.Context, stream *tramv1.Stream) error {
	engines := make([]tramv1.StreamEngineStatus, 0, len(stream.Spec.EngineRefs))
	for _, ref := range stream.Spec.EngineRefs {
		entry := tramv1.StreamEngineStatus{Name: ref.Name}
		for _, old := range stream.Status.Engines {
			if old.Name == ref.Name {
				entry.Conditions = append(entry.Conditions, old.Conditions...)
			}
		}

		accepted, err := r.accepted(ctx, stream, ref.Name)
		if err != nil {
			return err
		}
		meta.SetStatusCondition(&entry.Conditions, accepted)
		engines = append(engines, entry)
	}

	if equality.Semantic.DeepEqual(engines, stream.Status.Engines) ||
		(len(engines) == 0 && len(stream.Status.Engines) == 0) {
		return nil
	}
	patch := client.MergeFrom(stream.DeepCopy())
	stream.Status.Engines = engines
	if err := r.Status().Patch(ctx, stream, patch); err != nil {
		return fmt.Errorf("patch engine status: %w", err)
	}
	return nil
}

func (r *StreamReconciler) accepted(ctx context.Context, stream *tramv1.Stream, name string) (metav1.Condition, error) {
	condition := metav1.Condition{Type: tramv1.ConditionAccepted, ObservedGeneration: stream.Generation}

	var engine tramv1alpha1.Engine
	err := r.Get(ctx, types.NamespacedName{Namespace: stream.Namespace, Name: name}, &engine)
	switch {
	case apierrors.IsNotFound(err):
		condition.Status = metav1.ConditionFalse
		condition.Reason = "EngineNotFound"
		condition.Message = fmt.Sprintf("Engine %s does not exist in namespace %s", name, stream.Namespace)
	case err != nil:
		return metav1.Condition{}, fmt.Errorf("get engine %s: %w", name, err)
	case !engine.DeletionTimestamp.IsZero():
		condition.Status = metav1.ConditionFalse
		condition.Reason = "EngineDeleting"
		condition.Message = fmt.Sprintf("Engine %s is being deleted", name)
	default:
		condition.Status = metav1.ConditionTrue
		condition.Reason = "EngineFound"
		condition.Message = fmt.Sprintf("Engine %s runs the Stream", name)
	}
	return condition, nil
}

// SetupWithManager reconciles a Stream when its spec changes, e.g. its
// engineRefs, when its labels change, so engine labels edited by hand are
// restored, and when an Engine it refers to is created or deleted.
func (r *StreamReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&tramv1.Stream{}, builder.WithPredicates(predicate.Or[client.Object](
			predicate.GenerationChangedPredicate{},
			predicate.LabelChangedPredicate{},
		))).
		Watches(&tramv1alpha1.Engine{}, handler.EnqueueRequestsFromMapFunc(r.streamsOf),
			builder.WithPredicates(engineLifecycle())).
		Named("stream-engines").
		Complete(r)
}

// streamsOf returns the Streams that refer to engine, by their engine label.
func (r *StreamReconciler) streamsOf(ctx context.Context, engine client.Object) []reconcile.Request {
	var streams tramv1.StreamList
	if err := r.List(ctx, &streams,
		client.InNamespace(engine.GetNamespace()),
		client.HasLabels{tramv1.EngineLabel(engine.GetName())}); err != nil {
		logf.FromContext(ctx).Error(err, "Could not list Streams of Engine", "engine", engine.GetName())
		return nil
	}
	requests := make([]reconcile.Request, len(streams.Items))
	for i, s := range streams.Items {
		requests[i] = reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&s)}
	}
	return requests
}

// engineLifecycle passes the events that change whether an Engine accepts
// Streams: its creation, deletion and the start of its deletion.
func engineLifecycle() predicate.Funcs {
	return predicate.Funcs{
		UpdateFunc: func(e event.UpdateEvent) bool {
			return e.ObjectOld.GetDeletionTimestamp().IsZero() != e.ObjectNew.GetDeletionTimestamp().IsZero()
		},
		GenericFunc: func(event.GenericEvent) bool { return false },
	}
}

func engineNames(refs []tramv1.EngineReference) []string {
	names := make([]string, len(refs))
	for i, ref := range refs {
		names[i] = ref.Name
	}
	return names
}
