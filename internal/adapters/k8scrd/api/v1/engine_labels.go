// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package v1

import "strings"

// EngineLabelPrefix prefixes the labels that assign a Stream to Engines. The
// manager owns every label with this prefix: it derives them from
// spec.engineRefs and removes the others.
const EngineLabelPrefix = "engine.tram.io/"

// EngineLabel returns the label key that assigns a Stream to the Engine
// engine. Its value is empty; an Engine selects the key's existence.
func EngineLabel(engine string) string {
	return EngineLabelPrefix + engine
}

// WithEngineLabels returns a copy of labels with exactly one engine label per
// reference in refs: engine labels that no reference names are dropped, all
// other labels are kept. labels is not modified.
func WithEngineLabels(labels map[string]string, refs []EngineReference) map[string]string {
	out := make(map[string]string, len(labels)+len(refs))
	for key, value := range labels {
		if !strings.HasPrefix(key, EngineLabelPrefix) {
			out[key] = value
		}
	}
	for _, ref := range refs {
		out[EngineLabel(ref.Name)] = ""
	}
	return out
}

// RefersToEngine tells whether the Stream's spec.engineRefs name engine.
func (s *Stream) RefersToEngine(engine string) bool {
	for _, ref := range s.Spec.EngineRefs {
		if ref.Name == engine {
			return true
		}
	}
	return false
}
