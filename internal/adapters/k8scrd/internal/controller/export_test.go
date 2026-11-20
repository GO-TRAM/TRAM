// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package controller

import (
	"context"

	tramv1 "github.com/go-tram/tram/internal/adapters/k8scrd/api/v1"
	"github.com/go-tram/tram/internal/core/domain"
)

func ToDomain(name string, s tramv1.Stream) (*domain.StreamSpec, error) {
	return toDomain(name, s)
}

// ReplaceRefs replaces the secret references in value with replace's result
// for their name and key, and config map selectors with "configmaps:" and the
// selector.
func ReplaceRefs(value any, replace func(name, key string) any) (any, bool, error) {
	return replaceRefs(value, func(r ref) (any, error) {
		if r.configMaps != nil {
			return "configmaps:" + r.configMaps.String(), nil
		}
		return replace(r.secret.name, r.secret.key), nil
	})
}

// ConfigMapSelectors returns the config map selectors of a Stream resource as
// strings.
func ConfigMapSelectors(stream *tramv1.Stream) []string {
	var out []string
	for _, s := range configMapSelectors(stream) {
		out = append(out, s.String())
	}
	return out
}

// StreamsSelecting returns the names of the streams in namespace that select a
// config map with any of the label sets.
func (r *StreamReconciler) StreamsSelecting(
	ctx context.Context,
	namespace string,
	labelSets ...map[string]string,
) []string {
	var names []string
	for _, req := range r.streamsSelecting(ctx, namespace, labelSets...) {
		names = append(names, req.Name)
	}
	return names
}

const ConfigMapIndex = configMapIndex

func ConfigMapIndexValues(stream *tramv1.Stream) []string {
	if len(configMapSelectors(stream)) == 0 {
		return nil
	}
	return []string{configMapIndexValue}
}

func SecretNames(stream *tramv1.Stream) []string {
	return secretNames(stream)
}
