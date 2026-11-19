// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

// Package v1alpha1 contains API Schema definitions for the tram v1alpha1 API
// group.
// +kubebuilder:object:generate=true
// +groupName=io.tram.core
package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

//nolint:gochecknoglobals // Kubebuilder's scheme registration requires these package variables.
var (
	SchemeGroupVersion = schema.GroupVersion{Group: "io.tram.core", Version: "v1alpha1"}

	GroupVersion = SchemeGroupVersion

	SchemeBuilder = runtime.NewSchemeBuilder(func(scheme *runtime.Scheme) error {
		metav1.AddToGroupVersion(scheme, SchemeGroupVersion)
		return nil
	})

	AddToScheme = SchemeBuilder.AddToScheme
)
