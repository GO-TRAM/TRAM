// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package engines

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	appsv1ac "k8s.io/client-go/applyconfigurations/apps/v1"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	tramv1 "codeberg.org/tram/tram/internal/adapters/k8scrd/api/v1"
	tramv1alpha1 "codeberg.org/tram/tram/internal/adapters/k8scrd/api/v1alpha1"
)

// handoverFieldOwner co-owns the Deployment's replicas while they are handed
// over to the autoscaler, so the manager can stop applying them without
// resetting them; see replicas.
const handoverFieldOwner = "tram-manager-replicas-handover"

// EngineReconciler runs every Engine as a Deployment of Tram instances started
// with --engine, with a service account bound to ClusterRole in the Engine's
// namespace, a Service and, if the Engine asks for them, a
// HorizontalPodAutoscaler and a PodDisruptionBudget. All of them are applied
// server-side and owned by the Engine, so deleting it deletes them.
type EngineReconciler struct {
	client.Client

	// Image runs the Engines that set no spec.image.
	Image string
	// ClusterRole is bound to the service account of every Engine in its
	// namespace: the role of a Tram instance with the operator enabled.
	ClusterRole string
}

// +kubebuilder:rbac:groups=io.tram.core,resources=engines,verbs=get;list;watch
// +kubebuilder:rbac:groups=io.tram.core,resources=engines/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=io.tram.core,resources=engines/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services;serviceaccounts,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=rolebindings,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=clusterroles,verbs=bind,resourceNames=manager-role
// +kubebuilder:rbac:groups=autoscaling,resources=horizontalpodautoscalers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=get;list;watch;create;update;patch;delete

func (r *EngineReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var engine tramv1alpha1.Engine
	if err := r.Get(ctx, req.NamespacedName, &engine); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !engine.DeletionTimestamp.IsZero() {
		// The garbage collector deletes the owned resources.
		return ctrl.Result{}, nil
	}
	before := engine.DeepCopy()

	var deployment appsv1.Deployment
	key := types.NamespacedName{Namespace: engine.Namespace, Name: ResourceName(engine.Name)}
	found := true
	if err := r.Get(ctx, key, &deployment); apierrors.IsNotFound(err) {
		found = false
	} else if err != nil {
		return ctrl.Result{}, fmt.Errorf("get deployment: %w", err)
	}

	applyErr := r.applyAll(ctx, &engine, &deployment, found)
	reconciled := metav1.Condition{
		Type:               tramv1alpha1.ConditionReconciled,
		Status:             metav1.ConditionTrue,
		Reason:             "Applied",
		Message:            "The resources of the Engine match its spec",
		ObservedGeneration: engine.Generation,
	}
	switch {
	case errors.Is(applyErr, ErrInvalidEngine):
		reconciled.Status, reconciled.Reason, reconciled.Message = metav1.ConditionFalse, "InvalidSpec", applyErr.Error()
		// Retrying cannot fix the spec; a change of it reconciles again.
		applyErr = nil
	case applyErr != nil:
		reconciled.Status, reconciled.Reason, reconciled.Message = metav1.ConditionFalse, "ApplyFailed", applyErr.Error()
	}
	meta.SetStatusCondition(&engine.Status.Conditions, reconciled)

	if err := r.observe(ctx, &engine, &deployment, found); err != nil {
		return ctrl.Result{}, errors.Join(applyErr, err)
	}
	return ctrl.Result{}, errors.Join(applyErr, r.patchStatus(ctx, before, &engine))
}

// applyAll applies the Engine's resources and deletes the autoscaler and the
// disruption budget once the Engine no longer asks for them.
func (r *EngineReconciler) applyAll(
	ctx context.Context,
	engine *tramv1alpha1.Engine,
	deployment *appsv1.Deployment,
	found bool,
) error {
	params := buildParams{image: r.Image, clusterRole: r.ClusterRole}
	replicas, apply, err := r.replicas(ctx, engine, deployment, found)
	if err != nil {
		return err
	}
	if apply {
		params.replicas = &replicas
	}
	res, err := buildResources(engine, params)
	if err != nil {
		return err
	}

	opts := []client.ApplyOption{client.FieldOwner(FieldOwner), client.ForceOwnership}
	applies := []namedApply{
		{"service account", res.serviceAccount},
		{"role binding", res.roleBinding},
		{"deployment", res.deployment},
		{"service", res.service},
	}
	if res.hpa != nil {
		applies = append(applies, namedApply{"horizontal pod autoscaler", res.hpa})
	}
	if res.pdb != nil {
		applies = append(applies, namedApply{"pod disruption budget", res.pdb})
	}
	for _, a := range applies {
		if err = r.Apply(ctx, a.config, opts...); err != nil {
			return fmt.Errorf("apply %s: %w", a.kind, err)
		}
	}

	if res.hpa == nil {
		if err = r.deleteOwned(ctx, engine, &autoscalingv2.HorizontalPodAutoscaler{}); err != nil {
			return err
		}
	}
	if res.pdb == nil {
		if err = r.deleteOwned(ctx, engine, &policyv1.PodDisruptionBudget{}); err != nil {
			return err
		}
	}
	return nil
}

type namedApply struct {
	kind   string
	config runtime.ApplyConfiguration
}

// replicas returns the replicas to apply to the Deployment, spec.replicas,
// and whether to apply them at all: not while the autoscaler sets them. A new
// autoscaled Deployment starts with minReplicas.
//
// Dropping the replicas from an apply would reset them to 1 if the manager is
// their only owner, so before it stops applying them, a second field manager
// applies their current value. Once the autoscaler changes them, it owns them,
// and this apply conflicts, which is fine.
func (r *EngineReconciler) replicas(
	ctx context.Context,
	engine *tramv1alpha1.Engine,
	deployment *appsv1.Deployment,
	found bool,
) (int32, bool, error) {
	if !autoscaled(engine) {
		if engine.Spec.Replicas != nil {
			return *engine.Spec.Replicas, true, nil
		}
		return 1, true, nil
	}
	if !found {
		return *engine.Spec.Autoscaling.MinReplicas, true, nil
	}
	if deployment.Spec.Replicas == nil || !ownsReplicas(deployment, FieldOwner) {
		return 0, false, nil
	}

	handover := appsv1ac.Deployment(deployment.Name, deployment.Namespace).
		WithSpec(appsv1ac.DeploymentSpec().WithReplicas(*deployment.Spec.Replicas))
	if err := r.Apply(ctx, handover, client.FieldOwner(handoverFieldOwner)); err != nil && !apierrors.IsConflict(err) {
		return 0, false, fmt.Errorf("hand replicas over to the autoscaler: %w", err)
	}
	return 0, false, nil
}

// ownsReplicas tells whether manager applied the Deployment's replicas.
func ownsReplicas(deployment *appsv1.Deployment, manager string) bool {
	for _, entry := range deployment.ManagedFields {
		if entry.Manager != manager || entry.Operation != metav1.ManagedFieldsOperationApply || entry.FieldsV1 == nil {
			continue
		}
		var fields map[string]map[string]any
		if err := json.Unmarshal(entry.FieldsV1.GetRawBytes(), &fields); err != nil {
			continue
		}
		if _, ok := fields["f:spec"]["f:replicas"]; ok {
			return true
		}
	}
	return false
}

// deleteOwned deletes the Engine's object of obj's kind if the Engine controls
// it.
func (r *EngineReconciler) deleteOwned(ctx context.Context, engine *tramv1alpha1.Engine, obj client.Object) error {
	key := types.NamespacedName{Namespace: engine.Namespace, Name: ResourceName(engine.Name)}
	if err := r.Get(ctx, key, obj); err != nil {
		return client.IgnoreNotFound(err)
	}
	if !metav1.IsControlledBy(obj, engine) {
		return nil
	}
	if err := r.Delete(ctx, obj); client.IgnoreNotFound(err) != nil {
		return fmt.Errorf("delete %s: %w", strings.ToLower(fmt.Sprintf("%T", obj)), err)
	}
	logf.FromContext(ctx).Info("Deleted resource the Engine no longer asks for",
		"engine", client.ObjectKeyFromObject(engine), "resource", fmt.Sprintf("%T", obj))
	return nil
}

// observe fills the status from the Deployment and the Engine's Streams.
func (r *EngineReconciler) observe(
	ctx context.Context,
	engine *tramv1alpha1.Engine,
	deployment *appsv1.Deployment,
	found bool,
) error {
	var streams tramv1.StreamList
	if err := r.List(ctx, &streams,
		client.InNamespace(engine.Namespace),
		client.HasLabels{tramv1.EngineLabel(engine.Name)}); err != nil {
		return fmt.Errorf("list streams: %w", err)
	}

	status := &engine.Status
	status.ObservedGeneration = engine.Generation
	status.Selector = EngineLabel + "=" + engine.Name
	status.Streams = int32(len(streams.Items)) //nolint:gosec // the number of streams fits.
	status.Replicas, status.ReadyReplicas = 0, 0

	ready := metav1.Condition{
		Type:               tramv1alpha1.ConditionReady,
		Status:             metav1.ConditionFalse,
		Reason:             "DeploymentMissing",
		Message:            "The Deployment of the Engine does not exist yet",
		ObservedGeneration: engine.Generation,
	}
	if found {
		status.Replicas = deployment.Status.Replicas
		status.ReadyReplicas = deployment.Status.ReadyReplicas
		ready.Reason, ready.Message = "DeploymentUnavailable", "The Deployment of the Engine reports no availability yet"
		for _, c := range deployment.Status.Conditions {
			if c.Type != appsv1.DeploymentAvailable {
				continue
			}
			ready.Message = c.Message
			if c.Status == corev1.ConditionTrue {
				ready.Status, ready.Reason = metav1.ConditionTrue, "DeploymentAvailable"
			}
		}
	}
	meta.SetStatusCondition(&status.Conditions, ready)
	return nil
}

func (r *EngineReconciler) patchStatus(ctx context.Context, before, engine *tramv1alpha1.Engine) error {
	if equality.Semantic.DeepEqual(before.Status, engine.Status) {
		return nil
	}
	if err := r.Status().Patch(ctx, engine, client.MergeFrom(before)); err != nil {
		return fmt.Errorf("patch status: %w", err)
	}
	return nil
}

// SetupWithManager reconciles an Engine when its spec changes, when one of its
// resources changes, e.g. the Deployment's status or a resource deleted by
// hand, and when a Stream joins or leaves it.
func (r *EngineReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&tramv1alpha1.Engine{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.ServiceAccount{}).
		Owns(&rbacv1.RoleBinding{}).
		Owns(&autoscalingv2.HorizontalPodAutoscaler{}).
		Owns(&policyv1.PodDisruptionBudget{}).
		Watches(&tramv1.Stream{}, streamEngines()).
		Named("engine").
		Complete(r)
}

// streamEngines enqueues the Engines a Stream is labeled for, before and after
// a change of its labels, so their stream counts follow.
func streamEngines() handler.EventHandler {
	enqueue := func(q workqueue.TypedRateLimitingInterface[reconcile.Request], objs ...client.Object) {
		for _, obj := range objs {
			for key := range obj.GetLabels() {
				if name, ok := strings.CutPrefix(key, tramv1.EngineLabelPrefix); ok {
					q.Add(reconcile.Request{Namespace: obj.GetNamespace(), Name: name})
				}
			}
		}
	}
	return handler.Funcs{
		CreateFunc: func(_ context.Context, e event.CreateEvent, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			enqueue(q, e.Object)
		},
		UpdateFunc: func(_ context.Context, e event.UpdateEvent, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			if !equality.Semantic.DeepEqual(e.ObjectOld.GetLabels(), e.ObjectNew.GetLabels()) {
				enqueue(q, e.ObjectOld, e.ObjectNew)
			}
		},
		DeleteFunc: func(_ context.Context, e event.DeleteEvent, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			enqueue(q, e.Object)
		},
	}
}
