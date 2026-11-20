// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package v1_test

import (
	"maps"
	"testing"

	"github.com/stretchr/testify/assert"

	tramv1 "codeberg.org/tram/tram/internal/adapters/k8scrd/api/v1"
)

const mapperLabel = "engine.tram.io/mapper"

func refs(names ...string) []tramv1.EngineReference {
	out := make([]tramv1.EngineReference, len(names))
	for i, name := range names {
		out[i] = tramv1.EngineReference{Name: name}
	}
	return out
}

func TestWithEngineLabels(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		labels map[string]string
		refs   []tramv1.EngineReference
		want   map[string]string
	}{
		{
			name: "no labels and no references",
			want: map[string]string{},
		},
		{
			name: "adds a label per reference",
			refs: refs("mapper", "routers"),
			want: map[string]string{mapperLabel: "", "engine.tram.io/routers": ""},
		},
		{
			name:   "keeps other labels",
			labels: map[string]string{"team": "telemetry", "tram.io/validation": "true"},
			refs:   refs("mapper"),
			want: map[string]string{
				"team": "telemetry", "tram.io/validation": "true", mapperLabel: "",
			},
		},
		{
			name: "drops engine labels no reference names",
			labels: map[string]string{
				"engine.tram.io/old": "",
				mapperLabel:          "",
				"team":               "telemetry",
			},
			refs: refs("mapper"),
			want: map[string]string{mapperLabel: "", "team": "telemetry"},
		},
		{
			name:   "resets the value of an engine label set by hand",
			labels: map[string]string{mapperLabel: "yes"},
			refs:   refs("mapper"),
			want:   map[string]string{mapperLabel: ""},
		},
		{
			name:   "drops all engine labels without references",
			labels: map[string]string{mapperLabel: "", "team": "telemetry"},
			want:   map[string]string{"team": "telemetry"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			before := maps.Clone(tt.labels)

			assert.Equal(t, tt.want, tramv1.WithEngineLabels(tt.labels, tt.refs))
			assert.Equal(t, before, tt.labels, "the input is not modified")
		})
	}
}
