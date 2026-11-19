// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package engines

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"strconv"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	appsv1ac "k8s.io/client-go/applyconfigurations/apps/v1"
	autoscalingv2ac "k8s.io/client-go/applyconfigurations/autoscaling/v2"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	metav1ac "k8s.io/client-go/applyconfigurations/meta/v1"
	policyv1ac "k8s.io/client-go/applyconfigurations/policy/v1"
	rbacv1ac "k8s.io/client-go/applyconfigurations/rbac/v1"

	tramv1alpha1 "codeberg.org/eljoth/tram/internal/adapters/k8scrd/api/v1alpha1"
)

const (
	// EngineLabel carries the Engine's name on every resource the manager
	// generates for it, and selects the Engine's pods.
	EngineLabel = "tram.io/engine"

	// ManagedByLabel marks the resources the manager generates; the manager
	// caches only those.
	ManagedByLabel = "app.kubernetes.io/managed-by"
	// FieldOwner is the field manager of the manager's server-side applies,
	// and the value of ManagedByLabel.
	FieldOwner = "tram-manager"

	containerName = "tram"

	defaultHTTPPort         = 8080
	defaultGRPCPort         = 50051
	defaultTargetCPUPercent = 80
	defaultLogLevel         = "info"
)

// ErrInvalidEngine is the reason of an Engine whose spec cannot be run, e.g.
// one with colliding ports.
var ErrInvalidEngine = errors.New("invalid Engine")

// ResourceName names the resources the manager generates for engine.
func ResourceName(engine string) string {
	return "tram-engine-" + engine
}

// resources are the apply configurations of an Engine's resources. HPA and
// PDB are nil when the Engine does not ask for them.
type resources struct {
	serviceAccount *corev1ac.ServiceAccountApplyConfiguration
	roleBinding    *rbacv1ac.RoleBindingApplyConfiguration
	deployment     *appsv1ac.DeploymentApplyConfiguration
	service        *corev1ac.ServiceApplyConfiguration
	hpa            *autoscalingv2ac.HorizontalPodAutoscalerApplyConfiguration
	pdb            *policyv1ac.PodDisruptionBudgetApplyConfiguration
}

// buildParams are the inputs of buildResources besides the Engine.
type buildParams struct {
	// image runs when the Engine sets none.
	image string
	// clusterRole is bound to the Engine's service account in its namespace.
	clusterRole string
	// replicas of the Deployment; nil leaves them to the autoscaler.
	replicas *int32
}

// buildResources returns the resources that run engine. It fails with
// ErrInvalidEngine when the Engine has no image or its ports collide.
func buildResources(engine *tramv1alpha1.Engine, params buildParams) (resources, error) {
	image := engine.Spec.Image
	if image == "" {
		image = params.image
	}
	if image == "" {
		return resources{}, fmt.Errorf("%w: no image; set spec.image or the manager's --engine-image", ErrInvalidEngine)
	}
	ports, err := portsOf(engine)
	if err != nil {
		return resources{}, err
	}

	name, ns := ResourceName(engine.Name), engine.Namespace
	labels := managedLabels(engine.Name)
	owner := ownerOf(engine)

	podSpec, err := podSpecOf(engine, image, name, ports)
	if err != nil {
		return resources{}, err
	}
	podLabels := maps.Clone(engine.Spec.PodTemplate.Metadata.Labels)
	if podLabels == nil {
		podLabels = map[string]string{}
	}
	maps.Copy(podLabels, labels)

	deploymentSpec := appsv1ac.DeploymentSpec().
		WithSelector(metav1ac.LabelSelector().WithMatchLabels(map[string]string{EngineLabel: engine.Name})).
		WithTemplate(corev1ac.PodTemplateSpec().
			WithLabels(podLabels).
			WithAnnotations(engine.Spec.PodTemplate.Metadata.Annotations).
			WithSpec(podSpec))
	if params.replicas != nil {
		deploymentSpec.WithReplicas(*params.replicas)
	}

	res := resources{
		serviceAccount: corev1ac.ServiceAccount(name, ns).WithLabels(labels).WithOwnerReferences(owner),
		roleBinding: rbacv1ac.RoleBinding(name, ns).WithLabels(labels).WithOwnerReferences(owner).
			WithRoleRef(rbacv1ac.RoleRef().
				WithAPIGroup(rbacv1.GroupName).WithKind("ClusterRole").WithName(params.clusterRole)).
			WithSubjects(rbacv1ac.Subject().WithKind(rbacv1.ServiceAccountKind).WithName(name).WithNamespace(ns)),
		deployment: appsv1ac.Deployment(name, ns).WithLabels(labels).WithOwnerReferences(owner).
			WithSpec(deploymentSpec),
		service: serviceOf(engine, name, labels, owner, ports),
		hpa:     hpaOf(engine, name, labels, owner),
		pdb:     pdbOf(engine, name, labels, owner),
	}
	return res, nil
}

// managedLabels are the labels of every generated resource and pod.
func managedLabels(engine string) map[string]string {
	return map[string]string{
		EngineLabel:                   engine,
		"app.kubernetes.io/name":      "tram",
		"app.kubernetes.io/instance":  engine,
		"app.kubernetes.io/component": "engine",
		ManagedByLabel:                FieldOwner,
	}
}

func ownerOf(engine *tramv1alpha1.Engine) *metav1ac.OwnerReferenceApplyConfiguration {
	return metav1ac.OwnerReference().
		WithAPIVersion(tramv1alpha1.GroupVersion.String()).
		WithKind("Engine").
		WithName(engine.Name).
		WithUID(engine.UID).
		WithController(true).
		WithBlockOwnerDeletion(true)
}

// port is a port of the tram container, exposed by the Service unless
// internal.
type port struct {
	name     string
	number   int32
	internal bool
}

// portsOf returns the ports of the tram container: the APIs, pprof and the
// Engine's ports. Two ports with the same number fail.
func portsOf(engine *tramv1alpha1.Engine) ([]port, error) {
	api := engine.Spec.Runtime.API
	ports := []port{
		{name: "http", number: orDefault(api.HTTPPort, defaultHTTPPort)},
		{name: "grpc", number: orDefault(api.GRPCPort, defaultGRPCPort)},
	}
	if pprof := engine.Spec.Runtime.Pprof; pprof != nil {
		ports = append(ports, port{name: "pprof", number: pprof.Port, internal: true})
	}
	for _, p := range engine.Spec.Ports {
		ports = append(ports, port{name: p.Name, number: p.Port})
	}

	seen := make(map[int32]string, len(ports))
	for _, p := range ports {
		if other, ok := seen[p.number]; ok {
			return nil, fmt.Errorf("%w: ports %s and %s both use %d", ErrInvalidEngine, other, p.name, p.number)
		}
		seen[p.number] = p.name
	}
	return ports, nil
}

func orDefault(value, def int32) int32 {
	if value == 0 {
		return def
	}
	return value
}

// args are the arguments of `tram`, the image's entrypoint. The APIs listen on
// all interfaces to be reachable through the Service; pprof only on the pod's
// localhost, reachable with kubectl port-forward.
func args(engine *tramv1alpha1.Engine, ports []port) []string {
	logLevel := engine.Spec.Runtime.LogLevel
	if logLevel == "" {
		logLevel = defaultLogLevel
	}
	out := []string{
		"server",
		"--log-level=" + logLevel,
		"--http-endpoint=0.0.0.0:" + strconv.Itoa(int(ports[0].number)),
		"--grpc-endpoint=0.0.0.0:" + strconv.Itoa(int(ports[1].number)),
	}
	if pprof := engine.Spec.Runtime.Pprof; pprof != nil {
		out = append(out, "--pprof-endpoint=127.0.0.1:"+strconv.Itoa(int(pprof.Port)))
	}
	return append(out,
		"--enable-k8s-operator",
		"--engine="+engine.Name,
		"--watch-namespace="+engine.Namespace,
	)
}

func podSpecOf(
	engine *tramv1alpha1.Engine,
	image, serviceAccount string,
	ports []port,
) (*corev1ac.PodSpecApplyConfiguration, error) {
	container := corev1ac.Container().
		WithName(containerName).
		WithImage(image).
		WithArgs(args(engine, ports)...).
		WithReadinessProbe(corev1ac.Probe().
			WithTCPSocket(corev1ac.TCPSocketAction().WithPort(intstr.FromString("http")))).
		WithSecurityContext(corev1ac.SecurityContext().
			WithAllowPrivilegeEscalation(false).
			WithCapabilities(corev1ac.Capabilities().WithDrop("ALL")).
			WithSeccompProfile(corev1ac.SeccompProfile().WithType(corev1.SeccompProfileTypeRuntimeDefault)))
	if engine.Spec.ImagePullPolicy != "" {
		container.WithImagePullPolicy(engine.Spec.ImagePullPolicy)
	}
	for _, p := range ports {
		if p.internal {
			continue
		}
		container.WithPorts(corev1ac.ContainerPort().
			WithName(p.name).WithContainerPort(p.number).WithProtocol(corev1.ProtocolTCP))
	}
	resources, err := convert[corev1ac.ResourceRequirementsApplyConfiguration](engine.Spec.Resources)
	if err != nil {
		return nil, err
	}
	container.WithResources(resources)

	pod := engine.Spec.PodTemplate.Spec
	spec := corev1ac.PodSpec().
		WithServiceAccountName(serviceAccount).
		WithContainers(container)
	if pod.TerminationGracePeriodSeconds != nil {
		spec.WithTerminationGracePeriodSeconds(*pod.TerminationGracePeriodSeconds)
	}
	if pod.PriorityClassName != "" {
		spec.WithPriorityClassName(pod.PriorityClassName)
	}
	if len(pod.NodeSelector) > 0 {
		spec.WithNodeSelector(pod.NodeSelector)
	}
	if err = convertInto(pod.Tolerations, &spec.Tolerations); err != nil {
		return nil, err
	}
	if err = convertInto(pod.TopologySpreadConstraints, &spec.TopologySpreadConstraints); err != nil {
		return nil, err
	}
	if err = convertInto(pod.ImagePullSecrets, &spec.ImagePullSecrets); err != nil {
		return nil, err
	}
	if pod.Affinity != nil {
		if spec.Affinity, err = convert[corev1ac.AffinityApplyConfiguration](pod.Affinity); err != nil {
			return nil, err
		}
	}
	if pod.SecurityContext != nil {
		if spec.SecurityContext, err = convert[corev1ac.PodSecurityContextApplyConfiguration](
			pod.SecurityContext); err != nil {
			return nil, err
		}
	}
	return spec, nil
}

func serviceOf(
	engine *tramv1alpha1.Engine,
	name string,
	labels map[string]string,
	owner *metav1ac.OwnerReferenceApplyConfiguration,
	ports []port,
) *corev1ac.ServiceApplyConfiguration {
	serviceType := engine.Spec.Service.Type
	if serviceType == "" {
		serviceType = corev1.ServiceTypeClusterIP
	}
	spec := corev1ac.ServiceSpec().
		WithType(serviceType).
		WithSelector(map[string]string{EngineLabel: engine.Name})
	for _, p := range ports {
		if p.internal {
			continue
		}
		spec.WithPorts(corev1ac.ServicePort().
			WithName(p.name).WithPort(p.number).WithTargetPort(intstr.FromString(p.name)).
			WithProtocol(corev1.ProtocolTCP))
	}
	return corev1ac.Service(name, engine.Namespace).WithLabels(labels).WithOwnerReferences(owner).WithSpec(spec)
}

func hpaOf(
	engine *tramv1alpha1.Engine,
	name string,
	labels map[string]string,
	owner *metav1ac.OwnerReferenceApplyConfiguration,
) *autoscalingv2ac.HorizontalPodAutoscalerApplyConfiguration {
	if !autoscaled(engine) {
		return nil
	}
	scaling := engine.Spec.Autoscaling
	target := int32(defaultTargetCPUPercent)
	if scaling.TargetCPUUtilizationPercentage != nil {
		target = *scaling.TargetCPUUtilizationPercentage
	}
	return autoscalingv2ac.HorizontalPodAutoscaler(name, engine.Namespace).
		WithLabels(labels).WithOwnerReferences(owner).
		WithSpec(autoscalingv2ac.HorizontalPodAutoscalerSpec().
			WithScaleTargetRef(autoscalingv2ac.CrossVersionObjectReference().
				WithAPIVersion(appsv1.SchemeGroupVersion.String()).WithKind("Deployment").WithName(name)).
			WithMinReplicas(*scaling.MinReplicas).
			WithMaxReplicas(*scaling.MaxReplicas).
			WithMetrics(autoscalingv2ac.MetricSpec().
				WithType(autoscalingv2.ResourceMetricSourceType).
				WithResource(autoscalingv2ac.ResourceMetricSource().
					WithName(corev1.ResourceCPU).
					WithTarget(autoscalingv2ac.MetricTarget().
						WithType(autoscalingv2.UtilizationMetricType).
						WithAverageUtilization(target)))))
}

func pdbOf(
	engine *tramv1alpha1.Engine,
	name string,
	labels map[string]string,
	owner *metav1ac.OwnerReferenceApplyConfiguration,
) *policyv1ac.PodDisruptionBudgetApplyConfiguration {
	budget := engine.Spec.DisruptionBudget
	if budget == nil {
		return nil
	}
	spec := policyv1ac.PodDisruptionBudgetSpec().
		WithSelector(metav1ac.LabelSelector().WithMatchLabels(map[string]string{EngineLabel: engine.Name}))
	if budget.MinAvailable != nil {
		spec.WithMinAvailable(*budget.MinAvailable)
	}
	if budget.MaxUnavailable != nil {
		spec.WithMaxUnavailable(*budget.MaxUnavailable)
	}
	return policyv1ac.PodDisruptionBudget(name, engine.Namespace).
		WithLabels(labels).WithOwnerReferences(owner).WithSpec(spec)
}

// autoscaled tells whether a HorizontalPodAutoscaler sets the replicas of
// engine. The CRD requires the bounds while autoscaling is enabled.
func autoscaled(engine *tramv1alpha1.Engine) bool {
	scaling := engine.Spec.Autoscaling
	return scaling != nil && scaling.Enabled && scaling.MinReplicas != nil && scaling.MaxReplicas != nil
}

// convert turns a typed value from the Engine's spec into its apply
// configuration, which has the same JSON form.
func convert[T any](in any) (*T, error) {
	data, err := json.Marshal(in)
	if err != nil {
		return nil, fmt.Errorf("marshal %T: %w", in, err)
	}
	out := new(T)
	if err = json.Unmarshal(data, out); err != nil {
		return nil, fmt.Errorf("convert %T: %w", in, err)
	}
	return out, nil
}

// convertInto converts a list from the Engine's spec into the list of apply
// configurations out, leaving it nil when in is empty.
func convertInto[In, Out any](in []In, out *[]Out) error {
	if len(in) == 0 {
		return nil
	}
	converted, err := convert[[]Out](in)
	if err != nil {
		return err
	}
	*out = *converted
	return nil
}
