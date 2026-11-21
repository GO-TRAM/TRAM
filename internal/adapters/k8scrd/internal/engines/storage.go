// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package engines

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	appsv1ac "k8s.io/client-go/applyconfigurations/apps/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	tramv1 "github.com/go-tram/tram/internal/adapters/k8scrd/api/v1"
	tramv1alpha1 "github.com/go-tram/tram/internal/adapters/k8scrd/api/v1alpha1"
)

const (
	// EngineFinalizer keeps an Engine until the volumes of its instances are
	// drained, so deleting it discards no buffered events.
	EngineFinalizer = "tram.io/drain-buffers"

	// drainDeadlineMargin is added to the largest max_age of an Engine's
	// buffers: once a drain Job is older, everything on its volume has
	// expired, and the volume is deleted even if the Job never finished, e.g.
	// because its pod could not start.
	drainDeadlineMargin = time.Hour
	// waitForPod is how long the manager waits before it looks again whether
	// the instance of a volume is gone.
	waitForPod = 10 * time.Second

	defaultDrainCPU    = "50m"
	defaultDrainMemory = "128Mi"
)

// engineState is what exists of an Engine's resources.
type engineState struct {
	deployment  *appsv1.Deployment
	statefulSet *appsv1.StatefulSet
	claims      []corev1.PersistentVolumeClaim
	// jobs are the drain Jobs by the ordinal of their volume.
	jobs    map[int]*batchv1.Job
	streams []tramv1.Stream
}

// load reads the Engine's resources the manager controls.
func (r *EngineReconciler) load(ctx context.Context, engine *tramv1alpha1.Engine) (*engineState, error) {
	state := &engineState{jobs: map[int]*batchv1.Job{}}
	key := types.NamespacedName{Namespace: engine.Namespace, Name: ResourceName(engine.Name)}

	var deployment appsv1.Deployment
	switch err := r.Get(ctx, key, &deployment); {
	case err == nil && metav1.IsControlledBy(&deployment, engine):
		state.deployment = &deployment
	case client.IgnoreNotFound(err) != nil:
		return nil, fmt.Errorf("get deployment: %w", err)
	}
	var statefulSet appsv1.StatefulSet
	switch err := r.Get(ctx, key, &statefulSet); {
	case err == nil && metav1.IsControlledBy(&statefulSet, engine):
		state.statefulSet = &statefulSet
	case client.IgnoreNotFound(err) != nil:
		return nil, fmt.Errorf("get stateful set: %w", err)
	}

	var claims corev1.PersistentVolumeClaimList
	if err := r.List(ctx, &claims, client.InNamespace(engine.Namespace),
		client.MatchingLabels{EngineLabel: engine.Name, VolumeLabel: volumeName}); err != nil {
		return nil, fmt.Errorf("list volume claims: %w", err)
	}
	for _, claim := range claims.Items {
		if _, ok := claimOrdinal(engine.Name, claim.Name); ok && claim.DeletionTimestamp.IsZero() {
			state.claims = append(state.claims, claim)
		}
	}

	var jobs batchv1.JobList
	if err := r.List(ctx, &jobs, client.InNamespace(engine.Namespace),
		client.MatchingLabels{DrainLabel: engine.Name}); err != nil {
		return nil, fmt.Errorf("list drain jobs: %w", err)
	}
	for i := range jobs.Items {
		job := &jobs.Items[i]
		ordinal, err := strconv.Atoi(job.Labels[OrdinalLabel])
		if err != nil || !metav1.IsControlledBy(job, engine) || !job.DeletionTimestamp.IsZero() {
			continue
		}
		state.jobs[ordinal] = job
	}

	var streams tramv1.StreamList
	if err := r.List(ctx, &streams, client.InNamespace(engine.Namespace),
		client.HasLabels{tramv1.EngineLabel(engine.Name)}); err != nil {
		return nil, fmt.Errorf("list streams: %w", err)
	}
	state.streams = streams.Items
	return state, nil
}

// workload is a Deployment or StatefulSet of an Engine.
type workload struct {
	kind        string
	deployment  *appsv1.Deployment
	statefulSet *appsv1.StatefulSet
}

// workloads returns the workload the Engine should run on, and the one of
// the other kind it migrates from; nil for what does not exist.
func (s *engineState) workloads(engine *tramv1alpha1.Engine) (*workload, *workload) {
	var deployment, statefulSet *workload
	if s.deployment != nil {
		deployment = &workload{kind: tramv1alpha1.WorkloadDeployment, deployment: s.deployment}
	}
	if s.statefulSet != nil {
		statefulSet = &workload{kind: tramv1alpha1.WorkloadStatefulSet, statefulSet: s.statefulSet}
	}
	if engine.Spec.Storage != nil {
		return statefulSet, deployment
	}
	return deployment, statefulSet
}

func (w *workload) object() client.Object {
	if w.deployment != nil {
		return w.deployment
	}
	return w.statefulSet
}

func (w *workload) replicas() *int32 {
	if w.deployment != nil {
		return w.deployment.Spec.Replicas
	}
	return w.statefulSet.Spec.Replicas
}

func (w *workload) specReplicas() int32 {
	if replicas := w.replicas(); replicas != nil {
		return *replicas
	}
	return 1
}

func (w *workload) statusReplicas() (int32, int32) {
	if w.deployment != nil {
		return w.deployment.Status.Replicas, w.deployment.Status.ReadyReplicas
	}
	return w.statefulSet.Status.Replicas, w.statefulSet.Status.ReadyReplicas
}

// ready reports whether the workload is available: a Deployment by its
// Available condition, a StatefulSet once it runs its current spec with all
// its replicas ready.
func (w *workload) ready() bool {
	if w.deployment != nil {
		for _, c := range w.deployment.Status.Conditions {
			if c.Type == appsv1.DeploymentAvailable {
				return c.Status == corev1.ConditionTrue
			}
		}
		return false
	}
	status := w.statefulSet.Status
	return status.ObservedGeneration >= w.statefulSet.Generation && status.ReadyReplicas >= w.specReplicas()
}

// replicasApply applies the current replicas, to hand them over to the
// autoscaler.
func (w *workload) replicasApply() runtime.ApplyConfiguration {
	replicas := w.specReplicas()
	if w.deployment != nil {
		return appsv1ac.Deployment(w.deployment.Name, w.deployment.Namespace).
			WithSpec(appsv1ac.DeploymentSpec().WithReplicas(replicas))
	}
	return appsv1ac.StatefulSet(w.statefulSet.Name, w.statefulSet.Namespace).
		WithSpec(appsv1ac.StatefulSetSpec().WithReplicas(replicas))
}

// streamsNeedingStorage returns the names of the Engine's Streams with a
// buffer sink or source.
func (s *engineState) streamsNeedingStorage() []string {
	var names []string
	for i := range s.streams {
		if usesBuffer(&s.streams[i]) {
			names = append(names, s.streams[i].Name)
		}
	}
	return names
}

// storageServes reports whether the Service should select only the instances
// with storage: the Engine has storage, and its StatefulSet is ready or there
// is no Deployment to serve meanwhile. While a migration to storage waits for
// the StatefulSet, both serve.
func (s *engineState) storageServes(engine *tramv1alpha1.Engine) bool {
	if engine.Spec.Storage == nil {
		return false
	}
	if s.deployment == nil {
		return true
	}
	target, _ := s.workloads(engine)
	return target != nil && target.ready()
}

// migrate deletes the workload of the other kind once the one the Engine
// runs on is ready, after spec.storage was added or removed. Until then both
// run: the Service sends traffic to the pods of both, and once a StatefulSet
// is ready, only to its pods, before the Deployment goes. Volumes of a deleted
// StatefulSet stay and are drained.
func (r *EngineReconciler) migrate(ctx context.Context, engine *tramv1alpha1.Engine, state *engineState) error {
	target, other := state.workloads(engine)
	if other == nil || target == nil || !target.ready() {
		return nil
	}
	if err := r.Delete(ctx, other.object()); client.IgnoreNotFound(err) != nil {
		return fmt.Errorf("delete %s after migrating: %w", other.kind, err)
	}
	logf.FromContext(ctx).Info("Migrated Engine", "engine", client.ObjectKeyFromObject(engine),
		"from", other.kind, "to", target.kind)
	if other.kind == tramv1alpha1.WorkloadDeployment {
		state.deployment = nil
	} else {
		state.statefulSet = nil
	}
	return nil
}

// drainResult is what drain found.
type drainResult struct {
	// draining counts the volumes without an instance.
	draining int
	failed   int
	// requeue is when to look again; zero if a watch will tell.
	requeue time.Duration
}

func (d *drainResult) requeueAfter(after time.Duration) {
	if after <= 0 {
		after = time.Second
	}
	if d.requeue == 0 || after < d.requeue {
		d.requeue = after
	}
}

// drain makes sure that every volume of the Engine either belongs to a
// running instance or has a drain Job, and deletes volumes whose buffer files
// are drained or expired. A volume belongs to an instance while the
// StatefulSet exists and its ordinal is below the replicas; after scaling
// down, removing storage or deleting the Engine, it does not.
func (r *EngineReconciler) drain(
	ctx context.Context,
	engine *tramv1alpha1.Engine,
	state *engineState,
	deleting bool,
) (drainResult, error) {
	var result drainResult
	var errs []error
	discard := engine.Annotations[tramv1alpha1.DiscardBuffersAnnotation] == labelTrue
	deadline := maxBufferAge(state.streams) + drainDeadlineMargin
	log := logf.FromContext(ctx).WithValues("engine", client.ObjectKeyFromObject(engine))

	for i := range state.claims {
		claim := &state.claims[i]
		ordinal, _ := claimOrdinal(engine.Name, claim.Name)
		job := state.jobs[ordinal]

		if !deleting && state.ownsVolume(engine, ordinal) {
			if job != nil {
				// Scaled up again: the instance continues with its volume.
				errs = append(errs, r.deleteJob(ctx, job))
			}
			continue
		}
		if discard {
			log.Info("Discarding volume as the Engine asks", "claim", claim.Name)
			errs = append(errs, r.deleteVolume(ctx, claim, job))
			continue
		}

		result.draining++
		gone, err := r.instanceGone(ctx, engine, ordinal)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if !gone {
			result.requeueAfter(waitForPod)
			continue
		}
		if job == nil {
			errs = append(errs, r.createDrainJob(ctx, engine, ordinal))
			continue
		}
		switch {
		case jobCondition(job, batchv1.JobComplete):
			log.Info("Volume drained", "claim", claim.Name)
			result.draining--
			errs = append(errs, r.deleteVolume(ctx, claim, job))
		case time.Since(job.CreationTimestamp.Time) >= deadline:
			log.Info("Deleting volume whose buffer files have expired without being drained",
				"claim", claim.Name, "deadline", deadline)
			result.draining--
			errs = append(errs, r.deleteVolume(ctx, claim, job))
		default:
			if jobCondition(job, batchv1.JobFailed) {
				result.failed++
			}
			result.requeueAfter(deadline - time.Since(job.CreationTimestamp.Time))
		}
	}
	return result, errors.Join(errs...)
}

// ownsVolume reports whether an instance runs with the volume of ordinal.
func (s *engineState) ownsVolume(engine *tramv1alpha1.Engine, ordinal int) bool {
	set := s.statefulSet
	if engine.Spec.Storage == nil || set == nil || !set.DeletionTimestamp.IsZero() {
		return false
	}
	replicas := int32(1)
	if set.Spec.Replicas != nil {
		replicas = *set.Spec.Replicas
	}
	return int32(ordinal) < replicas //nolint:gosec // Ordinals of volumes fit.
}

// instanceGone reports whether the instance of a volume has stopped, so a
// drain Job can mount the volume.
func (r *EngineReconciler) instanceGone(ctx context.Context, engine *tramv1alpha1.Engine, ordinal int) (bool, error) {
	key := types.NamespacedName{
		Namespace: engine.Namespace,
		Name:      ResourceName(engine.Name) + "-" + strconv.Itoa(ordinal),
	}
	err := r.Get(ctx, key, &corev1.Pod{})
	if apierrors.IsNotFound(err) {
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("get instance: %w", err)
	}
	return false, nil
}

func (r *EngineReconciler) createDrainJob(ctx context.Context, engine *tramv1alpha1.Engine, ordinal int) error {
	job, err := drainJobOf(engine, r.Image, ordinal, r.Scheme())
	if err != nil {
		return err
	}
	if err = r.Create(ctx, job); err != nil {
		return fmt.Errorf("create drain job: %w", err)
	}
	logf.FromContext(ctx).Info("Draining volume without an instance",
		"engine", client.ObjectKeyFromObject(engine), "claim", ClaimName(engine.Name, ordinal), "job", job.Name)
	return nil
}

func (r *EngineReconciler) deleteJob(ctx context.Context, job *batchv1.Job) error {
	err := r.Delete(ctx, job, client.PropagationPolicy(metav1.DeletePropagationBackground))
	if client.IgnoreNotFound(err) != nil {
		return fmt.Errorf("delete drain job: %w", err)
	}
	return nil
}

// deleteVolume deletes a drained volume, and its drain Job first, so the
// volume is no longer in use.
func (r *EngineReconciler) deleteVolume(
	ctx context.Context,
	claim *corev1.PersistentVolumeClaim,
	job *batchv1.Job,
) error {
	if job != nil {
		if err := r.deleteJob(ctx, job); err != nil {
			return err
		}
	}
	if err := r.Delete(ctx, claim); client.IgnoreNotFound(err) != nil {
		return fmt.Errorf("delete volume claim: %w", err)
	}
	return nil
}

func jobCondition(job *batchv1.Job, condition batchv1.JobConditionType) bool {
	for _, c := range job.Status.Conditions {
		if c.Type == condition && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// ensureFinalizer keeps the Engine on deletion while it has storage or
// volumes, so they are drained first.
func (r *EngineReconciler) ensureFinalizer(
	ctx context.Context,
	engine *tramv1alpha1.Engine,
	state *engineState,
) error {
	if engine.Spec.Storage == nil && len(state.claims) == 0 {
		return nil
	}
	if controllerutil.ContainsFinalizer(engine, EngineFinalizer) {
		return nil
	}
	patch := client.MergeFrom(engine.DeepCopy())
	controllerutil.AddFinalizer(engine, EngineFinalizer)
	if err := r.Patch(ctx, engine, patch); err != nil {
		return fmt.Errorf("add finalizer: %w", err)
	}
	return nil
}

// finalize drains the volumes of an Engine being deleted: the instances stop,
// every volume gets a drain Job, and the Engine goes once no volume is left.
func (r *EngineReconciler) finalize(
	ctx context.Context,
	engine *tramv1alpha1.Engine,
	state *engineState,
) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(engine, EngineFinalizer) {
		// The garbage collector deletes the owned resources.
		return ctrl.Result{}, nil
	}
	// The garbage collector deletes owned resources only once the Engine is
	// gone, so the instances are stopped here to free their volumes.
	var instances []client.Object
	if state.deployment != nil && state.deployment.DeletionTimestamp.IsZero() {
		instances = append(instances, state.deployment)
	}
	if state.statefulSet != nil && state.statefulSet.DeletionTimestamp.IsZero() {
		instances = append(instances, state.statefulSet)
	}
	for _, obj := range instances {
		if err := r.Delete(ctx, obj); client.IgnoreNotFound(err) != nil {
			return ctrl.Result{}, fmt.Errorf("stop instances: %w", err)
		}
	}

	drained, err := r.drain(ctx, engine, state, true)
	if err != nil {
		return ctrl.Result{}, err
	}
	if drained.draining > 0 {
		return ctrl.Result{RequeueAfter: drained.requeue}, nil
	}
	patch := client.MergeFrom(engine.DeepCopy())
	controllerutil.RemoveFinalizer(engine, EngineFinalizer)
	if err = r.Patch(ctx, engine, patch); err != nil {
		return ctrl.Result{}, fmt.Errorf("remove finalizer: %w", err)
	}
	logf.FromContext(ctx).Info("Volumes of deleted Engine drained", "engine", client.ObjectKeyFromObject(engine))
	return ctrl.Result{}, nil
}

// claimEngine enqueues the Engine of a volume claim.
func claimEngine(_ context.Context, obj client.Object) []reconcile.Request {
	engine, ok := obj.GetLabels()[EngineLabel]
	if !ok || obj.GetLabels()[VolumeLabel] != volumeName {
		return nil
	}
	return []reconcile.Request{{Namespace: obj.GetNamespace(), Name: engine}}
}

// drainJobOf is the Job that drains the volume of ordinal: `tram drain` with
// the volume as data directory, run as the Engine's service account. Its pod
// carries no EngineLabel, so the Service and the disruption budget leave it
// alone. Job names are generated, since the Engine's name already fills the
// length allowed for them.
func drainJobOf(
	engine *tramv1alpha1.Engine,
	defaultImage string,
	ordinal int,
	scheme *runtime.Scheme,
) (*batchv1.Job, error) {
	image := engine.Spec.Image
	if image == "" {
		image = defaultImage
	}
	if image == "" {
		return nil, fmt.Errorf("%w: no image; set spec.image or the manager's --engine-image", ErrInvalidEngine)
	}
	labels := map[string]string{
		DrainLabel:                    engine.Name,
		OrdinalLabel:                  strconv.Itoa(ordinal),
		ManagedByLabel:                FieldOwner,
		"app.kubernetes.io/name":      "tram",
		"app.kubernetes.io/instance":  engine.Name,
		"app.kubernetes.io/component": "drain",
	}
	logLevel := engine.Spec.Runtime.LogLevel
	if logLevel == "" {
		logLevel = defaultLogLevel
	}

	pod := engine.Spec.PodTemplate.Spec
	security := &corev1.PodSecurityContext{}
	if pod.SecurityContext != nil {
		security = pod.SecurityContext.DeepCopy()
	}
	if security.FSGroup == nil {
		security.FSGroup = new(int64(nonRootGroup))
	}
	resources, err := drainResourcesOf(engine)
	if err != nil {
		return nil, err
	}

	job := &batchv1.Job{
		GenerateName: "tram-drain-",
		Namespace:    engine.Namespace,
		Labels:       labels,
		Spec: batchv1.JobSpec{
			BackoffLimit: new(int32(drainBackoffLimit)),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					RestartPolicy:      corev1.RestartPolicyOnFailure,
					ServiceAccountName: ResourceName(engine.Name),
					SecurityContext:    security,
					PriorityClassName:  pod.PriorityClassName,
					NodeSelector:       pod.NodeSelector,
					Tolerations:        pod.Tolerations,
					ImagePullSecrets:   pod.ImagePullSecrets,
					Containers: []corev1.Container{{
						Name:            containerName,
						Image:           image,
						ImagePullPolicy: engine.Spec.ImagePullPolicy,
						Args: []string{
							"drain",
							"--log-level=" + logLevel,
							"--engine=" + engine.Name,
							"--watch-namespace=" + engine.Namespace,
							"--data-dir=" + drainMountPath,
						},
						Resources:    resources,
						VolumeMounts: []corev1.VolumeMount{{Name: volumeName, MountPath: drainMountPath}},
						SecurityContext: &corev1.SecurityContext{
							AllowPrivilegeEscalation: new(false),
							Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
							SeccompProfile: &corev1.SeccompProfile{
								Type: corev1.SeccompProfileTypeRuntimeDefault,
							},
						},
					}},
					Volumes: []corev1.Volume{
						{
							Name: volumeName,
							PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
								ClaimName: ClaimName(engine.Name, ordinal),
							},
						},
					},
				},
			},
		},
	}
	if err = controllerutil.SetControllerReference(engine, job, scheme); err != nil {
		return nil, err
	}
	return job, nil
}

// drainResourcesOf are spec.storage.drainResources, or small requests with
// the instances' limits.
func drainResourcesOf(engine *tramv1alpha1.Engine) (corev1.ResourceRequirements, error) {
	if storage := engine.Spec.Storage; storage != nil && storage.DrainResources != nil {
		return *storage.DrainResources.DeepCopy(), nil
	}
	cpu, err := resource.ParseQuantity(defaultDrainCPU)
	if err != nil {
		return corev1.ResourceRequirements{}, err
	}
	memory, err := resource.ParseQuantity(defaultDrainMemory)
	if err != nil {
		return corev1.ResourceRequirements{}, err
	}
	requests := make(corev1.ResourceList)
	requests[corev1.ResourceCPU] = cpu
	requests[corev1.ResourceMemory] = memory
	return corev1.ResourceRequirements{Requests: requests, Limits: engine.Spec.Resources.Limits.DeepCopy()}, nil
}
