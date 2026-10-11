// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

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

package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

type Spec struct {
	Name string `json:"name"`
	Type string `json:"type"`
	// Spec is the component specific configuration. It is passed through
	// unchanged, so numbers, booleans and nested objects keep their types.
	// +kubebuilder:pruning:PreserveUnknownFields
	// +kubebuilder:validation:Type=object
	Spec *runtime.RawExtension `json:"spec,omitempty"`
}

type ReceiverSpec struct {
	Name string `json:"name"`
	Type string `json:"type"`
	// Spec is the component specific configuration. It is passed through
	// unchanged, so numbers, booleans and nested objects keep their types.
	// +kubebuilder:pruning:PreserveUnknownFields
	// +kubebuilder:validation:Type=object
	Spec *runtime.RawExtension `json:"spec,omitempty"`
	From string                `json:"from"`
}

type StreamSpec struct {
	Source Spec           `json:"source"`
	Stages []ReceiverSpec `json:"stages,omitempty"`
	Sinks  []ReceiverSpec `json:"sinks"`
}

type StreamStatus struct {
	Active bool `json:"active"`
}

// +genclient
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Active",type=boolean,JSONPath=`.status.active`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

type Stream struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   StreamSpec   `json:"spec,omitempty"`
	Status StreamStatus `json:"status,omitempty"`
}

// StreamList is a list of Stream resources.
// +kubebuilder:object:root=true
type StreamList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []Stream `json:"items"`
}

//nolint:gochecknoinits // Registers the types with SchemeBuilder, as Kubebuilder expects.
func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &Stream{}, &StreamList{})
		return nil
	})
}
