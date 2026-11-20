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

	"github.com/go-tram/tram/internal/testrun"
)

func TestLoadStream(t *testing.T) {
	t.Parallel()

	stream := `{
		"name": "s",
		"source": {"name": "in", "type": "otlp", "spec": {"http_port": 4318}},
		"sinks": [{"name": "out", "type": "debug", "from": "in"}],
		"metrics": {"request_size": true},
		"tests": [{"name": "t", "input": {"logs": {}}, "expect": {"out": "drop"}}]
	}`
	for name, content := range map[string]string{
		"bare":    stream,
		"wrapped": `{"stream": ` + stream + `}`,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			path := filepath.Join(t.TempDir(), "stream.json")
			require.NoError(t, os.WriteFile(path, []byte(content), 0o600))

			spec, err := testrun.LoadStream(path)
			require.NoError(t, err)
			assert.Equal(t, "s", spec.Name)
			assert.Equal(t, "otlp", spec.Source.Type)
			require.Len(t, spec.Sinks, 1)
			require.NotNil(t, spec.Metrics.RequestSize, "snake_case fields are read")
			assert.True(t, *spec.Metrics.RequestSize)
			require.Len(t, spec.Tests, 1)
			assert.Equal(t, "drop", spec.Tests[0].Expect["out"])
		})
	}
}
