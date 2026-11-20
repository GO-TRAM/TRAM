// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package domain_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"codeberg.org/tram/tram/internal/core/domain"
)

func TestNodeMetricsDefaults(t *testing.T) {
	t.Parallel()

	var unset domain.NodeMetrics
	assert.True(t, unset.Enabled(domain.KindSource))
	assert.False(t, unset.Enabled(domain.KindStage))
	assert.True(t, unset.Enabled(domain.KindSink))
	assert.True(t, domain.MetricsSpec{}.RequestSizeEnabled())

	set := domain.NodeMetrics{Source: new(false), Stages: new(true), Sinks: new(false)}
	assert.False(t, set.Enabled(domain.KindSource))
	assert.True(t, set.Enabled(domain.KindStage))
	assert.False(t, set.Enabled(domain.KindSink))
	assert.False(t, domain.MetricsSpec{RequestSize: new(false)}.RequestSizeEnabled())
}

func TestMetricsSpecEqualComparesEffectiveSettings(t *testing.T) {
	t.Parallel()

	defaults := domain.MetricsSpec{}
	explicitDefaults := domain.MetricsSpec{
		Records:     domain.NodeMetrics{Source: new(true), Stages: new(false), Sinks: new(true)},
		Duration:    domain.NodeMetrics{Source: new(true), Stages: new(false), Sinks: new(true)},
		RequestSize: new(true),
	}
	assert.True(t, defaults.Equal(explicitDefaults), "an unset field equals its default")

	for name, changed := range map[string]domain.MetricsSpec{
		"records":      {Records: domain.NodeMetrics{Stages: new(true)}},
		"duration":     {Duration: domain.NodeMetrics{Sinks: new(false)}},
		"request size": {RequestSize: new(false)},
	} {
		assert.False(t, defaults.Equal(changed), name)
	}
}

func TestStreamSpecEqualComparesMetrics(t *testing.T) {
	t.Parallel()

	a := domain.StreamSpec{Name: "s", Source: domain.SourceSpec{Name: "src", Type: "otlp"}}
	b := a
	b.Metrics.Duration.Stages = new(true)
	assert.False(t, a.Equal(b), "a change of the metrics is a change of the stream")
}
