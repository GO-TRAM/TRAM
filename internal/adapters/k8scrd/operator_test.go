// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package k8scrd_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"codeberg.org/eljoth/tram/internal/adapters/k8scrd"
)

func TestCacheOptionsWithoutLimitsWatchesEverything(t *testing.T) {
	t.Parallel()

	opts, err := k8scrd.CacheOptions(k8scrd.Options{})
	require.NoError(t, err)
	assert.Nil(t, opts.DefaultNamespaces)
	assert.Nil(t, opts.DefaultLabelSelector)
}

func TestCacheOptionsLimitsNamespaces(t *testing.T) {
	t.Parallel()

	opts, err := k8scrd.CacheOptions(k8scrd.Options{Namespaces: []string{"a", "b"}})
	require.NoError(t, err)
	assert.Len(t, opts.DefaultNamespaces, 2)
	assert.Contains(t, opts.DefaultNamespaces, "a")
	assert.Contains(t, opts.DefaultNamespaces, "b")
}

func TestCacheOptionsRejectsEmptyNamespace(t *testing.T) {
	t.Parallel()

	_, err := k8scrd.CacheOptions(k8scrd.Options{Namespaces: []string{"a", ""}})
	require.ErrorIs(t, err, k8scrd.ErrEmptyNamespace)
}

func TestCacheOptionsRejectsInvalidSelector(t *testing.T) {
	t.Parallel()

	_, err := k8scrd.CacheOptions(k8scrd.Options{StreamSelector: "tram.io/instance in (a"})
	require.ErrorIs(t, err, k8scrd.ErrInvalidStreamSelector)
}

func TestCacheOptionsSelectorAssignsStreams(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		selector string
		labels   labels.Set
		want     bool
	}{
		{"key per deployment matches own key", "tram.io/validation=true",
			labels.Set{"tram.io/validation": "true"}, true},
		{"key per deployment matches stream with both keys", "tram.io/bridge=true",
			labels.Set{"tram.io/validation": "true", "tram.io/bridge": "true"}, true},
		{"key per deployment ignores other key", "tram.io/bridge=true",
			labels.Set{"tram.io/validation": "true"}, false},
		{"set-based matches shared value", "tram.io/instance in (bridge,shared)",
			labels.Set{"tram.io/instance": "shared"}, true},
		{"set-based ignores other instance", "tram.io/instance in (bridge,shared)",
			labels.Set{"tram.io/instance": "validation"}, false},
		{"unlabeled stream is not run", "tram.io/validation=true",
			labels.Set{}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			opts, err := k8scrd.CacheOptions(k8scrd.Options{StreamSelector: tt.selector})
			require.NoError(t, err)
			assert.Equal(t, tt.want, opts.DefaultLabelSelector.Matches(tt.labels))
		})
	}
}

func TestCacheOptionsEngineSelectsItsStreams(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		selector string
		labels   labels.Set
		want     bool
	}{
		{"stream of the engine", "", labels.Set{"engine.tram.io/mapper": ""}, true},
		{"engine label with any value", "", labels.Set{"engine.tram.io/mapper": "yes"}, true},
		{"stream of the engine and another", "",
			labels.Set{"engine.tram.io/mapper": "", "engine.tram.io/routers": ""}, true},
		{"stream of another engine", "", labels.Set{"engine.tram.io/routers": ""}, false},
		{"stream without engine", "", labels.Set{"tram.io/validation": "true"}, false},
		{"stream selector and engine both match", "team=telemetry",
			labels.Set{"engine.tram.io/mapper": "", "team": "telemetry"}, true},
		{"stream selector does not match", "team=telemetry",
			labels.Set{"engine.tram.io/mapper": "", "team": "other"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			opts, err := k8scrd.CacheOptions(k8scrd.Options{StreamSelector: tt.selector, Engine: "mapper"})
			require.NoError(t, err)
			assert.Equal(t, tt.want, opts.DefaultLabelSelector.Matches(tt.labels))
			assert.Contains(t, opts.ByObject, client.Object(&metav1.PartialObjectMetadata{
				APIVersion: "v1", Kind: "Secret",
			}), "secrets are watched without the engine's label")
		})
	}
}

func TestCacheOptionsRejectsInvalidEngine(t *testing.T) {
	t.Parallel()

	for _, engine := range []string{"Mapper", "mapper_v2", "-mapper", strings.Repeat("a", 64)} {
		_, err := k8scrd.CacheOptions(k8scrd.Options{Engine: engine})
		require.ErrorIs(t, err, k8scrd.ErrInvalidEngine, engine)
	}
}
