// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// Condition types of an Engine.
const (
	// ConditionReconciled tells whether the manager applied the latest spec.
	ConditionReconciled = "Reconciled"
	// ConditionReady tells whether the Engine's Deployment is available.
	ConditionReady = "Ready"
)

// EngineSpec describes a group of Tram instances. The manager runs them as a
// Deployment; every instance runs every Stream whose spec.engineRefs names the
// Engine.
type EngineSpec struct {
	// Replicas is the number of Tram instances while autoscaling is disabled.
	// +optional
	// +kubebuilder:default=1
	// +kubebuilder:validation:Minimum=0
	Replicas *int32 `json:"replicas,omitempty"`

	// Image is the Tram image of the instances. Defaults to the manager's
	// --engine-image.
	// +optional
	Image string `json:"image,omitempty"`

	// +optional
	// +kubebuilder:validation:Enum=Always;IfNotPresent;Never
	ImagePullPolicy corev1.PullPolicy `json:"imagePullPolicy,omitempty"`

	// Resources of the tram container.
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`

	// Autoscaling lets a HorizontalPodAutoscaler set the number of instances
	// by their CPU utilization.
	// +optional
	Autoscaling *AutoscalingSpec `json:"autoscaling,omitempty"`

	// DisruptionBudget limits how many instances voluntary disruptions, like
	// node drains, may take down at a time.
	// +optional
	DisruptionBudget *DisruptionBudgetSpec `json:"disruptionBudget,omitempty"`

	// Ports are the ports the HTTP sources of the Engine's Streams may listen
	// on. The Engine's Service exposes them.
	// +optional
	// +listType=map
	// +listMapKey=name
	// +kubebuilder:validation:MaxItems=32
	Ports []EnginePort `json:"ports,omitempty"`

	// +optional
	Service ServiceSpec `json:"service,omitempty"`

	// Runtime holds the options of the Tram process.
	// +optional
	Runtime RuntimeSpec `json:"runtime,omitempty"`

	// PodTemplate holds pod-level settings of the instances. The tram
	// container is configured by the other fields.
	// +optional
	PodTemplate PodTemplate `json:"podTemplate,omitempty"`
}

// AutoscalingSpec configures the HorizontalPodAutoscaler of an Engine.
// +kubebuilder:validation:XValidation:rule="!self.enabled || (has(self.minReplicas) && has(self.maxReplicas))",message="minReplicas and maxReplicas are required while autoscaling is enabled"
// +kubebuilder:validation:XValidation:rule="!has(self.minReplicas) || !has(self.maxReplicas) || self.minReplicas <= self.maxReplicas",message="minReplicas must not exceed maxReplicas"
type AutoscalingSpec struct {
	// Enabled creates the HorizontalPodAutoscaler; while it is enabled,
	// spec.replicas is not used.
	// +optional
	Enabled bool `json:"enabled,omitempty"`
	// +optional
	// +kubebuilder:validation:Minimum=1
	MinReplicas *int32 `json:"minReplicas,omitempty"`
	// +optional
	// +kubebuilder:validation:Minimum=1
	MaxReplicas *int32 `json:"maxReplicas,omitempty"`
	// TargetCPUUtilizationPercentage is the average CPU utilization, relative
	// to the CPU requests, the autoscaler aims at. Defaults to 80.
	// +optional
	// +kubebuilder:validation:Minimum=1
	TargetCPUUtilizationPercentage *int32 `json:"targetCPUUtilizationPercentage,omitempty"`
}

// DisruptionBudgetSpec configures the PodDisruptionBudget of an Engine.
// +kubebuilder:validation:XValidation:rule="has(self.minAvailable) != has(self.maxUnavailable)",message="set exactly one of minAvailable and maxUnavailable"
type DisruptionBudgetSpec struct {
	// +optional
	// +kubebuilder:validation:XIntOrString
	MinAvailable *intstr.IntOrString `json:"minAvailable,omitempty"`
	// +optional
	// +kubebuilder:validation:XIntOrString
	MaxUnavailable *intstr.IntOrString `json:"maxUnavailable,omitempty"`
}

// EnginePort is a port the HTTP sources of the Engine's Streams may listen on.
// +kubebuilder:validation:XValidation:rule="!(self.name in ['http', 'grpc', 'pprof'])",message="http, grpc and pprof are reserved for the ports of the Tram process"
type EnginePort struct {
	// Name of the port in the container and the Service.
	// +kubebuilder:validation:MaxLength=15
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	Name string `json:"name"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	Port int32 `json:"port"`
}

// ServiceSpec configures the Service of an Engine.
type ServiceSpec struct {
	// Type of the Service. Defaults to ClusterIP.
	// +optional
	// +kubebuilder:validation:Enum=ClusterIP;NodePort;LoadBalancer
	Type corev1.ServiceType `json:"type,omitempty"`
}

// RuntimeSpec maps to flags of `tram server`.
type RuntimeSpec struct {
	// LogLevel is --log-level. Defaults to info.
	// +optional
	// +kubebuilder:validation:Enum=debug;info;warn;error
	LogLevel string `json:"logLevel,omitempty"`
	// +optional
	API APISpec `json:"api,omitempty"`
	// Pprof serves the Go profiles on the port, bound to the pod's localhost:
	// reach it with kubectl port-forward. Disabled without it.
	// +optional
	Pprof *PprofSpec `json:"pprof,omitempty"`
}

// APISpec sets the ports of the Tram APIs.
type APISpec struct {
	// HTTPPort is the port of --http-endpoint, which also serves /metrics.
	// Defaults to 8080.
	// +optional
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	HTTPPort int32 `json:"httpPort,omitempty"`
	// GRPCPort is the port of --grpc-endpoint. Defaults to 50051.
	// +optional
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	GRPCPort int32 `json:"grpcPort,omitempty"`
}

// PprofSpec sets the port of --pprof-endpoint.
type PprofSpec struct {
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	Port int32 `json:"port"`
}

// PodTemplate holds the pod-level settings of an Engine's instances, with the
// names of a PodTemplateSpec.
type PodTemplate struct {
	// +optional
	Metadata PodMetadata `json:"metadata,omitempty"`
	// +optional
	Spec PodSpec `json:"spec,omitempty"`
}

// PodMetadata are labels and annotations added to the instances' pods.
// +kubebuilder:validation:XValidation:rule="!has(self.labels) || !self.labels.exists(k, k == 'tram.io/engine' || k.startsWith('app.kubernetes.io/'))",message="tram.io/engine and app.kubernetes.io/ labels are set by the manager"
type PodMetadata struct {
	// +optional
	Labels map[string]string `json:"labels,omitempty"`
	// +optional
	Annotations map[string]string `json:"annotations,omitempty"`
}

// PodSpec holds the pod-level fields of a PodSpec an Engine may set.
type PodSpec struct {
	// +optional
	// +kubebuilder:validation:Minimum=0
	TerminationGracePeriodSeconds *int64 `json:"terminationGracePeriodSeconds,omitempty"`
	// +optional
	PriorityClassName string `json:"priorityClassName,omitempty"`
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`
	// +optional
	// +listType=atomic
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`
	// +optional
	Affinity *corev1.Affinity `json:"affinity,omitempty"`
	// +optional
	// +listType=atomic
	TopologySpreadConstraints []corev1.TopologySpreadConstraint `json:"topologySpreadConstraints,omitempty"`
	// +optional
	SecurityContext *corev1.PodSecurityContext `json:"securityContext,omitempty"`
	// +optional
	// +listType=atomic
	ImagePullSecrets []corev1.LocalObjectReference `json:"imagePullSecrets,omitempty"`
}

// EngineStatus is the observed state of an Engine.
type EngineStatus struct {
	// ObservedGeneration is the generation the status describes.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// Replicas is the number of instances of the Deployment.
	// +optional
	Replicas int32 `json:"replicas,omitempty"`
	// ReadyReplicas is the number of ready instances.
	// +optional
	ReadyReplicas int32 `json:"readyReplicas,omitempty"`
	// Streams is the number of Streams whose spec.engineRefs name the Engine.
	// +optional
	Streams int32 `json:"streams,omitempty"`
	// Selector selects the instances' pods, for the scale subresource.
	// +optional
	Selector string `json:"selector,omitempty"`
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// Engine runs Tram instances for the Streams that refer to it.
// +genclient
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:subresource:scale:specpath=.spec.replicas,statuspath=.status.replicas,selectorpath=.status.selector
// +kubebuilder:printcolumn:name="Replicas",type=integer,JSONPath=`.status.replicas`
// +kubebuilder:printcolumn:name="Ready",type=integer,JSONPath=`.status.readyReplicas`
// +kubebuilder:printcolumn:name="Streams",type=integer,JSONPath=`.status.streams`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
// +kubebuilder:validation:XValidation:rule="self.metadata.name.size() <= 51 && self.metadata.name.matches('^[a-z0-9]([-a-z0-9]*[a-z0-9])?$')",message="the name must be a DNS-1123 label of at most 51 characters"
type Engine struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   EngineSpec   `json:"spec,omitempty"`
	Status EngineStatus `json:"status,omitempty"`
}

// EngineList is a list of Engine resources.
// +kubebuilder:object:root=true
type EngineList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []Engine `json:"items"`
}

//nolint:gochecknoinits // Registers the types with SchemeBuilder, as Kubebuilder expects.
func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &Engine{}, &EngineList{})
		return nil
	})
}
