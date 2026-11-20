// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package controller

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tramv1 "codeberg.org/tram/tram/internal/adapters/k8scrd/api/v1"
	"codeberg.org/tram/tram/internal/core/domain"
)

const (
	valueFromKey         = "valueFrom"
	secretKeyRefKey      = "secretKeyRef"
	configMapSelectorKey = "configMapSelector"

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

// ref is a reference to a value in the namespace of the stream: exactly one of
// its fields is set.
type ref struct {
	secret     *secretRef
	configMaps *configMapSelector
}

// refOf reports whether value is a reference:
// {"valueFrom": {"secretKeyRef": {"name": ..., "key": ...}}} or
// {"valueFrom": {"configMapSelector": {"matchLabels": ...}}}. Other objects
// with a valueFrom key are not references.
func refOf(value any) (ref, bool, error) {
	object, ok := value.(map[string]any)
	if !ok || len(object) != 1 {
		return ref{}, false, nil
	}
	valueFrom, ok := object[valueFromKey].(map[string]any)
	if !ok || len(valueFrom) != 1 {
		return ref{}, false, nil
	}
	if selector, isSelector := valueFrom[configMapSelectorKey]; isSelector {
		configMaps, err := configMapSelectorOf(selector)
		if err != nil {
			return ref{}, true, err
		}
		return ref{configMaps: configMaps}, true, nil
	}
	selector, ok := valueFrom[secretKeyRefKey]
	if !ok {
		return ref{}, false, nil
	}

	fields, ok := selector.(map[string]any)
	if !ok {
		return ref{}, true, fmt.Errorf("%w: secretKeyRef is not an object", ErrInvalidSecretRef)
	}
	name, _ := fields["name"].(string)
	key, _ := fields["key"].(string)
	if name == "" || key == "" || len(fields) != 2 {
		return ref{}, true, fmt.Errorf("%w: secretKeyRef needs exactly a name and a key", ErrInvalidSecretRef)
	}
	return ref{secret: &secretRef{name: name, key: key}}, true, nil
}

// replaceRefs returns a copy of value in which replace has replaced every
// reference, and whether there were any. value is not modified.
func replaceRefs(value any, replace func(ref) (any, error)) (any, bool, error) {
	r, isRef, err := refOf(value)
	if err != nil {
		return nil, true, err
	}
	if isRef {
		replaced, replaceErr := replace(r)
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
func specRefs(spec domain.StreamSpec, replace func(ref) (any, error)) (domain.StreamSpec, bool, error) {
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

// resolveRefs returns a copy of spec with the values of the secrets and config
// maps it refers to, read from namespace, and whether it refers to any.
func resolveRefs(
	ctx context.Context,
	reader client.Reader,
	namespace string,
	spec domain.StreamSpec,
) (domain.StreamSpec, bool, error) {
	secrets := map[string]*corev1.Secret{}
	tables := map[string]map[string]any{}
	return specRefs(spec, func(r ref) (any, error) {
		if r.configMaps != nil {
			// Copies, as components may keep and change their spec.
			if table, ok := tables[r.configMaps.String()]; ok {
				return maps.Clone(table), nil
			}
			table, err := resolveConfigMaps(ctx, reader, namespace, r.configMaps)
			if err != nil {
				return nil, err
			}
			tables[r.configMaps.String()] = table
			return maps.Clone(table), nil
		}
		return resolveSecret(ctx, reader, namespace, *r.secret, secrets)
	})
}

func resolveSecret(
	ctx context.Context,
	reader client.Reader,
	namespace string,
	ref secretRef,
	secrets map[string]*corev1.Secret,
) (any, error) {
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
}

// secretNames returns the names of the secrets a Stream resource refers to.
func secretNames(stream *tramv1.Stream) []string {
	var names []string
	for _, r := range streamRefs(stream) {
		if r.secret != nil && !slices.Contains(names, r.secret.name) {
			names = append(names, r.secret.name)
		}
	}
	return names
}

// streamRefs returns the valid references of a Stream resource.
func streamRefs(stream *tramv1.Stream) []ref {
	var refs []ref
	collect := func(raw *runtime.RawExtension) {
		spec, err := specToMap(raw)
		if err != nil {
			return
		}
		_, _, _ = replaceRefs(spec, func(r ref) (any, error) {
			refs = append(refs, r)
			// Only collected, so any value does.
			return "", nil
		})
	}
	collect(stream.Spec.Source.Spec)
	for _, stage := range stream.Spec.Stages {
		collect(stage.Spec)
	}
	for _, sink := range stream.Spec.Sinks {
		collect(sink.Spec)
	}
	return refs
}
