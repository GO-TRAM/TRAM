// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package domain_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"codeberg.org/tram/tram/internal/core/domain"
)

func TestValidateStreamName(t *testing.T) {
	t.Parallel()

	valid := []string{
		"stream",
		"otlp_basic",
		"Stream-2",
		"default.my-stream.v1",
	}
	for _, name := range valid {
		require.NoError(t, domain.ValidateStreamName(name), name)
	}

	invalid := []string{
		"",
		".",
		"..",
		"../escape",
		"a/b",
		`a\b`,
		"/etc/passwd",
		".hidden",
		"-flag",
		"with space",
		"nul\x00byte",
	}
	for _, name := range invalid {
		assert.ErrorIs(t, domain.ValidateStreamName(name), domain.ErrInvalidStreamName, name)
	}
}

func TestParseSpecRejectsUnknownFields(t *testing.T) {
	t.Parallel()

	type spec struct {
		MaxRecords int `json:"max_records"`
	}

	parsed, err := domain.ParseSpec(map[string]any{"max_records": 10}, new(spec))
	require.NoError(t, err)
	assert.Equal(t, 10, parsed.MaxRecords)

	_, err = domain.ParseSpec(map[string]any{"max_record": 10}, new(spec))
	assert.ErrorContains(t, err, "max_record")
}

func TestStreamSpecEqual(t *testing.T) {
	t.Parallel()

	base := func() domain.StreamSpec {
		return domain.StreamSpec{
			Name:   "s",
			Source: domain.SourceSpec{Name: "in", Type: "otlp", Spec: map[string]any{"http_port": float64(4318)}},
			Stages: []domain.StageSpec{
				{
					Name: "f",
					Type: "filter",
					From: "in",
					Spec: map[string]any{"driver": "cel", "cel": map[string]any{"logs": "true"}},
				},
			},
			Sinks: []domain.SinkSpec{{Name: "out", Type: "debug", From: "f"}},
		}
	}

	cases := []struct {
		name     string
		change   func(*domain.StreamSpec)
		expected bool
	}{
		{
			name: "identical",
			change: func(*domain.StreamSpec) {
				// Leaves the spec unchanged.
			},
			expected: true,
		},
		{
			name:     "removed stages",
			change:   func(s *domain.StreamSpec) { s.Stages = nil; s.Sinks[0].From = "in" },
			expected: false,
		},
		{
			name:     "empty instead of missing component spec",
			change:   func(s *domain.StreamSpec) { s.Sinks[0].Spec = map[string]any{} },
			expected: true,
		},
		{
			name:     "changed component spec",
			change:   func(s *domain.StreamSpec) { s.Source.Spec["http_port"] = float64(4319) },
			expected: false,
		},
		{
			name:     "number of another Go type",
			change:   func(s *domain.StreamSpec) { s.Source.Spec["http_port"] = int64(4318) },
			expected: true,
		},
		{
			name: "duplicate name instead of another stage",
			change: func(s *domain.StreamSpec) {
				s.Stages = append(s.Stages, s.Stages[0])
			},
			expected: false,
		},
		{name: "changed source type", change: func(s *domain.StreamSpec) { s.Source.Type = "nats" }, expected: false},
		{name: "changed from", change: func(s *domain.StreamSpec) { s.Sinks[0].From = "in" }, expected: false},
		{name: "added sink", change: func(s *domain.StreamSpec) {
			s.Sinks = append(s.Sinks, domain.SinkSpec{Name: "out2", Type: "debug", From: "f"})
		}, expected: false},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			changed := base()
			c.change(&changed)
			assert.Equal(t, c.expected, base().Equal(changed))
			assert.Equal(t, c.expected, changed.Equal(base()))
		})
	}
}

func TestStreamSpecEqualIgnoresOrder(t *testing.T) {
	t.Parallel()

	spec := domain.StreamSpec{
		Name:   "s",
		Source: domain.SourceSpec{Name: "in", Type: "generator"},
		Stages: []domain.StageSpec{
			{Name: "a", Type: "noop", From: "in"},
			{Name: "b", Type: "noop", From: "a"},
		},
		Sinks: []domain.SinkSpec{
			{Name: "x", Type: "debug", From: "a"},
			{Name: "y", Type: "debug", From: "b"},
		},
	}
	reordered := spec
	reordered.Stages = []domain.StageSpec{spec.Stages[1], spec.Stages[0]}
	reordered.Sinks = []domain.SinkSpec{spec.Sinks[1], spec.Sinks[0]}

	assert.True(t, spec.Equal(reordered))
	assert.True(t, reordered.Equal(spec))
}

func TestStreamSpecEqualTreatsMissingAndEmptyListsAlike(t *testing.T) {
	t.Parallel()

	withNil := domain.StreamSpec{Name: "s", Source: domain.SourceSpec{Name: "in", Type: "generator"}}
	withEmpty := domain.StreamSpec{
		Name:   "s",
		Source: domain.SourceSpec{Name: "in", Type: "generator", Spec: map[string]any{}},
		Stages: []domain.StageSpec{},
		Sinks:  []domain.SinkSpec{},
	}

	assert.True(t, withNil.Equal(withEmpty))
	assert.True(t, withNil.Source.Equal(withEmpty.Source))
}
