// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package configsync_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/go-tram/tram/internal/adapters/configsync"
	"github.com/go-tram/tram/internal/core/domain"
)

func TestStreamFileMapsMetrics(t *testing.T) {
	t.Parallel()

	var stream configsync.Stream
	require.NoError(t, json.Unmarshal([]byte(`{"stream": {
		"name": "s",
		"source": {"name": "in", "type": "otlp"},
		"sinks": [{"name": "out", "type": "nats", "from": "in"}],
		"metrics": {
			"records": {"stages": true},
			"duration": {"source": false},
			"request_size": false
		}
	}}`), &stream))

	want := domain.MetricsSpec{
		Records:     domain.NodeMetrics{Stages: new(true)},
		Duration:    domain.NodeMetrics{Source: new(false)},
		RequestSize: new(false),
	}
	assert.Equal(t, want, stream.AsDomain().Metrics)
	assert.Equal(t, stream.Stream.Metrics, configsync.StreamFromDomain(*stream.AsDomain()).Stream.Metrics,
		"writing the stream back keeps its metrics")
}

func TestStreamFileLeavesOutDefaultMetrics(t *testing.T) {
	t.Parallel()

	written, err := json.Marshal(configsync.StreamFromDomain(domain.StreamSpec{
		Name:   "s",
		Source: domain.SourceSpec{Name: "in", Type: "otlp"},
	}))
	require.NoError(t, err)

	assert.NotContains(t, string(written), "metrics", "a stream with the defaults writes no metrics")
}

func TestStreamFileMapsSuspended(t *testing.T) {
	t.Parallel()

	var stream configsync.Stream
	require.NoError(t, json.Unmarshal([]byte(`{"stream": {
		"name": "s",
		"suspended": true,
		"source": {"name": "in", "type": "otlp"},
		"sinks": [{"name": "out", "type": "nats", "from": "in"}]
	}}`), &stream))

	spec := stream.AsDomain()
	assert.True(t, spec.Suspended)
	assert.True(t, configsync.StreamFromDomain(*spec).Stream.Suspended != nil &&
		*configsync.StreamFromDomain(*spec).Stream.Suspended,
		"writing the stream back keeps suspended")

	written, err := json.Marshal(configsync.StreamFromDomain(domain.StreamSpec{
		Name:   "s",
		Source: domain.SourceSpec{Name: "in", Type: "otlp"},
	}))
	require.NoError(t, err)
	assert.NotContains(t, string(written), "suspended", "a running stream writes no suspended field")
}
