// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	tramv1 "codeberg.org/eljoth/tram/internal/adapters/k8scrd/api/v1"
)

// configMapIndex indexes Stream resources that select config maps.
const configMapIndex = "spec.configMapSelectors"

// configMapIndexValue is the index value of every stream that selects config
// maps; which ones it selects needs its selectors.
const configMapIndexValue = "true"

var (
	// ErrInvalidConfigMapSelector is a selector that resolving cannot fix by
	// retrying.
	ErrInvalidConfigMapSelector = errors.New("invalid config map selector")
	// ErrConfigMapNotFound may go away once a config map matching the selector
	// is created.
	ErrConfigMapNotFound = errors.New("no config map matches the selector")
	// ErrConfigMapConflict is a key with different values in two selected
	// config maps. It goes away once one of them is changed.
	ErrConfigMapConflict = errors.New("config maps disagree")
)

// configMapSelector selects the config maps whose data, merged, is the value
// of a reference.
type configMapSelector struct {
	labels.Selector
}

// configMapSelectorOf parses a label selector like the selector of a
// deployment: matchLabels and matchExpressions.
func configMapSelectorOf(value any) (*configMapSelector, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidConfigMapSelector, err)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var selector metav1.LabelSelector
	if err = decoder.Decode(&selector); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidConfigMapSelector, err)
	}
	// An empty selector would merge every config map of the namespace.
	if len(selector.MatchLabels) == 0 && len(selector.MatchExpressions) == 0 {
		return nil, fmt.Errorf("%w: needs matchLabels or matchExpressions", ErrInvalidConfigMapSelector)
	}
	parsed, err := metav1.LabelSelectorAsSelector(&selector)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidConfigMapSelector, err)
	}
	return &configMapSelector{Selector: parsed}, nil
}

// resolveConfigMaps merges the data of the config maps in namespace that
// selector matches. A key may be in several of them only with the same value.
func resolveConfigMaps(
	ctx context.Context,
	reader client.Reader,
	namespace string,
	selector *configMapSelector,
) (map[string]any, error) {
	var list corev1.ConfigMapList
	err := reader.List(ctx, &list, client.InNamespace(namespace),
		client.MatchingLabelsSelector{Selector: selector.Selector})
	if err != nil {
		return nil, fmt.Errorf("list config maps %s: %w", selector, err)
	}
	if len(list.Items) == 0 {
		return nil, fmt.Errorf("%w: %s in %s", ErrConfigMapNotFound, selector, namespace)
	}

	// Sorted, so a conflict names the same config maps every time.
	slices.SortFunc(list.Items, func(a, b corev1.ConfigMap) int { return strings.Compare(a.Name, b.Name) })
	size := 0
	for _, cm := range list.Items {
		size += len(cm.Data)
	}
	merged := make(map[string]any, size)
	from := make(map[string]string, size)
	for _, cm := range list.Items {
		for key, value := range cm.Data {
			if existing, ok := merged[key]; ok && existing != value {
				return nil, fmt.Errorf("%w: key %s is %q in %s and %q in %s",
					ErrConfigMapConflict, key, existing, from[key], value, cm.Name)
			}
			merged[key] = value
			from[key] = cm.Name
		}
	}
	return merged, nil
}

// configMapSelectors returns the config map selectors of a Stream resource.
func configMapSelectors(stream *tramv1.Stream) []*configMapSelector {
	var selectors []*configMapSelector
	for _, r := range streamRefs(stream) {
		if r.configMaps != nil {
			selectors = append(selectors, r.configMaps)
		}
	}
	return selectors
}

// streamsSelecting returns the Stream resources in the namespace of the config
// maps with any of the label sets, so a config map that is created, changed,
// deleted, or no longer matches after a label change updates their streams.
func (r *StreamReconciler) streamsSelecting(
	ctx context.Context,
	namespace string,
	labelSets ...map[string]string,
) []reconcile.Request {
	var streams tramv1.StreamList
	if err := r.List(ctx, &streams,
		client.InNamespace(namespace),
		client.MatchingFields{configMapIndex: configMapIndexValue}); err != nil {
		logf.FromContext(ctx).Error(err, "Could not list streams selecting config maps", "namespace", namespace)
		return nil
	}
	var requests []reconcile.Request
	for _, s := range streams.Items {
		if selectsAny(configMapSelectors(&s), labelSets) {
			requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&s)})
		}
	}
	return requests
}

func selectsAny(selectors []*configMapSelector, labelSets []map[string]string) bool {
	for _, selector := range selectors {
		for _, set := range labelSets {
			if selector.Matches(labels.Set(set)) {
				return true
			}
		}
	}
	return false
}
