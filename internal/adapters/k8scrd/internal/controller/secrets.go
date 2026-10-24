// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package controller

import (
	"context"
	"errors"
	"fmt"
	"slices"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tramv1 "codeberg.org/eljoth/tram/internal/adapters/k8scrd/api/v1"
	"codeberg.org/eljoth/tram/internal/core/domain"
)

const (
	valueFromKey    = "valueFrom"
	secretKeyRefKey = "secretKeyRef"

	// secretIndex indexes Stream resources by the secrets they refer to.
	secretIndex = "spec.secretRefs" //nolint:gosec // The name of an index, no credential.
)

var (
	// ErrInvalidSecretRef is a reference that resolving cannot fix by retrying.
	ErrInvalidSecretRef = errors.New("invalid secret reference")
	// ErrSecretNotFound may go away once the secret or its key is created.
	ErrSecretNotFound = errors.New("secret not found")
)

type secretRef struct {
	name string
	key  string
}

// refOf reports whether value is a secret reference:
// {"valueFrom": {"secretKeyRef": {"name": ..., "key": ...}}}. Other objects
// with a valueFrom key are not references.
func refOf(value any) (secretRef, bool, error) {
	object, ok := value.(map[string]any)
	if !ok || len(object) != 1 {
		return secretRef{}, false, nil
	}
	valueFrom, ok := object[valueFromKey].(map[string]any)
	if !ok || len(valueFrom) != 1 {
		return secretRef{}, false, nil
	}
	selector, ok := valueFrom[secretKeyRefKey]
	if !ok {
		return secretRef{}, false, nil
	}

	fields, ok := selector.(map[string]any)
	if !ok {
		return secretRef{}, true, fmt.Errorf("%w: secretKeyRef is not an object", ErrInvalidSecretRef)
	}
	name, _ := fields["name"].(string)
	key, _ := fields["key"].(string)
	if name == "" || key == "" || len(fields) != 2 {
		return secretRef{}, true, fmt.Errorf("%w: secretKeyRef needs exactly a name and a key", ErrInvalidSecretRef)
	}
	return secretRef{name: name, key: key}, true, nil
}

// replaceRefs returns a copy of value in which replace has replaced every
// secret reference, and whether there were any. value is not modified.
func replaceRefs(value any, replace func(secretRef) (any, error)) (any, bool, error) {
	ref, isRef, err := refOf(value)
	if err != nil {
		return nil, true, err
	}
	if isRef {
		replaced, replaceErr := replace(ref)
		return replaced, true, replaceErr
	}

	var found bool
	switch v := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(v))
		for key, item := range v {
			replaced, itemFound, itemErr := replaceRefs(item, replace)
			if itemErr != nil {
				return nil, true, fmt.Errorf("%s: %w", key, itemErr)
			}
			found = found || itemFound
			out[key] = replaced
		}
		return out, found, nil
	case []any:
		out := make([]any, len(v))
		for i, item := range v {
			replaced, itemFound, itemErr := replaceRefs(item, replace)
			if itemErr != nil {
				return nil, true, fmt.Errorf("[%d]: %w", i, itemErr)
			}
			found = found || itemFound
			out[i] = replaced
		}
		return out, found, nil
	default:
		return value, false, nil
	}
}

// specRefs walks the component specs of spec.
func specRefs(spec domain.StreamSpec, replace func(secretRef) (any, error)) (domain.StreamSpec, bool, error) {
	var found bool
	walk := func(component string, s map[string]any) (map[string]any, error) {
		if s == nil {
			return s, nil
		}
		replaced, componentFound, err := replaceRefs(s, replace)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", component, err)
		}
		found = found || componentFound
		m, _ := replaced.(map[string]any)
		return m, nil
	}

	out := spec
	var err error
	if out.Source.Spec, err = walk(spec.Source.Name, spec.Source.Spec); err != nil {
		return domain.StreamSpec{}, true, err
	}
	out.Stages = slices.Clone(spec.Stages)
	for i := range out.Stages {
		if out.Stages[i].Spec, err = walk(out.Stages[i].Name, out.Stages[i].Spec); err != nil {
			return domain.StreamSpec{}, true, err
		}
	}
	out.Sinks = slices.Clone(spec.Sinks)
	for i := range out.Sinks {
		if out.Sinks[i].Spec, err = walk(out.Sinks[i].Name, out.Sinks[i].Spec); err != nil {
			return domain.StreamSpec{}, true, err
		}
	}
	return out, found, nil
}

// resolveSecrets returns a copy of spec with the values of the secrets it
// refers to, read from namespace, and whether it refers to any.
func resolveSecrets(
	ctx context.Context,
	reader client.Reader,
	namespace string,
	spec domain.StreamSpec,
) (domain.StreamSpec, bool, error) {
	secrets := map[string]*corev1.Secret{}
	return specRefs(spec, func(ref secretRef) (any, error) {
		secret, ok := secrets[ref.name]
		if !ok {
			secret = &corev1.Secret{}
			err := reader.Get(ctx, types.NamespacedName{Namespace: namespace, Name: ref.name}, secret)
			if client.IgnoreNotFound(err) != nil {
				return nil, fmt.Errorf("get secret %s: %w", ref.name, err)
			}
			if err != nil {
				return nil, fmt.Errorf("%w: %s/%s", ErrSecretNotFound, namespace, ref.name)
			}
			secrets[ref.name] = secret
		}
		value, ok := secret.Data[ref.key]
		if !ok {
			return nil, fmt.Errorf("%w: %s/%s has no key %s", ErrSecretNotFound, namespace, ref.name, ref.key)
		}
		return string(value), nil
	})
}

// secretNames returns the names of the secrets a Stream resource refers to.
func secretNames(stream *tramv1.Stream) []string {
	var names []string
	collect := func(raw *runtime.RawExtension) {
		spec, err := specToMap(raw)
		if err != nil {
			return
		}
		_, _, _ = replaceRefs(spec, func(ref secretRef) (any, error) {
			if !slices.Contains(names, ref.name) {
				names = append(names, ref.name)
			}
			return ref.name, nil
		})
	}
	collect(stream.Spec.Source.Spec)
	for _, stage := range stream.Spec.Stages {
		collect(stage.Spec)
	}
	for _, sink := range stream.Spec.Sinks {
		collect(sink.Spec)
	}
	return names
}
