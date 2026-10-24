// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package controller

import (
	tramv1 "codeberg.org/eljoth/tram/internal/adapters/k8scrd/api/v1"
	"codeberg.org/eljoth/tram/internal/core/domain"
)

func ToDomain(name string, s tramv1.Stream) (*domain.StreamSpec, error) {
	return toDomain(name, s)
}

// ReplaceRefs replaces the secret references in value with replace's result
// for their name and key.
func ReplaceRefs(value any, replace func(name, key string) any) (any, bool, error) {
	return replaceRefs(value, func(ref secretRef) (any, error) { return replace(ref.name, ref.key), nil })
}

func SecretNames(stream *tramv1.Stream) []string {
	return secretNames(stream)
}
