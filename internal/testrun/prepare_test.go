// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package testrun_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"codeberg.org/eljoth/tram/internal/core/domain"
	"codeberg.org/eljoth/tram/internal/testrun"
)

func secretRef(name, key string) map[string]any {
	return map[string]any{
		"valueFrom": map[string]any{
			"secretKeyRef": map[string]any{"name": name, "key": key},
		},
	}
}

func TestPrepareResolvesStageRefs(t *testing.T) {
	t.Parallel()

	spec := domain.StreamSpec{
		Name: "s",
		Stages: []domain.StageSpec{{
			Name: "stage",
			Type: "some_stage",
			From: "in",
			Spec: map[string]any{
				"token": secretRef("my-secret", "token"),
				"nested": map[string]any{
					"list": []any{secretRef("other-secret", "key")},
				},
				"plain": "value",
			},
		}},
	}
	secrets := map[string]string{
		"my-secret/token":  "super-secret-value",
		"other-secret/key": "another-value",
	}

	prepared, err := testrun.Prepare(spec, secrets)
	require.NoError(t, err)

	stageSpec := prepared.Stages[0].Spec
	assert.Equal(t, "super-secret-value", stageSpec["token"])
	assert.Equal(t, "another-value", stageSpec["nested"].(map[string]any)["list"].([]any)[0])
	assert.Equal(t, "value", stageSpec["plain"])

	// The original spec is not modified.
	assert.Equal(t, secretRef("my-secret", "token"), spec.Stages[0].Spec["token"])
}

func TestPrepareMissingSecretNamesTheRef(t *testing.T) {
	t.Parallel()

	spec := domain.StreamSpec{
		Name: "s",
		Stages: []domain.StageSpec{{
			Name: "stage",
			Type: "some_stage",
			From: "in",
			Spec: map[string]any{"token": secretRef("my-secret", "token")},
		}},
	}

	_, err := testrun.Prepare(spec, map[string]string{})
	require.ErrorIs(t, err, testrun.ErrUnresolvableRef)
	assert.Contains(t, err.Error(), "stage")
	assert.Contains(t, err.Error(), "my-secret")
	assert.Contains(t, err.Error(), "token")
}

func TestPrepareConfigMapSelectorNotResolvable(t *testing.T) {
	t.Parallel()

	spec := domain.StreamSpec{
		Name: "s",
		Stages: []domain.StageSpec{{
			Name: "stage",
			Type: "some_stage",
			From: "in",
			Spec: map[string]any{
				"value": map[string]any{
					"valueFrom": map[string]any{
						"configMapSelector": map[string]any{"matchLabels": map[string]any{"app": "x"}},
					},
				},
			},
		}},
	}

	_, err := testrun.Prepare(spec, map[string]string{})
	require.ErrorIs(t, err, testrun.ErrUnresolvableRef)
	assert.Contains(t, err.Error(), "configMapSelector")
}

func TestPrepareLeavesSourceAndSinkSpecsUntouched(t *testing.T) {
	t.Parallel()

	spec := domain.StreamSpec{
		Name:   "s",
		Source: domain.SourceSpec{Name: "in", Type: "nats", Spec: map[string]any{"token": secretRef("s", "k")}},
		Sinks: []domain.SinkSpec{
			{Name: "out", Type: "nats", From: "in", Spec: map[string]any{"token": secretRef("s", "k")}},
		},
	}

	prepared, err := testrun.Prepare(spec, nil)
	require.NoError(t, err)
	assert.Equal(t, spec.Source, prepared.Source)
	assert.Equal(t, spec.Sinks, prepared.Sinks)
}

func TestLoadSecrets(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "secrets.json")
	require.NoError(t, os.WriteFile(path, []byte(`{"my-secret/token": "v"}`), 0o600))

	secrets, err := testrun.LoadSecrets(path)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"my-secret/token": "v"}, secrets)

	_, err = testrun.LoadSecrets(filepath.Join(t.TempDir(), "missing.json"))
	require.Error(t, err)

	bad := filepath.Join(t.TempDir(), "bad.json")
	require.NoError(t, os.WriteFile(bad, []byte(`not json`), 0o600))
	_, err = testrun.LoadSecrets(bad)
	require.Error(t, err)
}
