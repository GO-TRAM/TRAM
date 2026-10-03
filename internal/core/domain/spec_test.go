package domain_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"codeberg.org/eljoth/tram/internal/core/domain"
)

func TestStreamSpecEqual(t *testing.T) {
	t.Parallel()

	base := func() domain.StreamSpec {
		return domain.StreamSpec{
			Name:   "s",
			Source: domain.SourceSpec{Name: "in", Type: "otlp", Spec: map[string]any{"http_port": float64(4318)}},
			Stages: []domain.StageSpec{
				{Name: "f", Type: "cel_filter", From: "in", Spec: map[string]any{"logs": "true"}},
			},
			Sinks: []domain.SinkSpec{{Name: "out", Type: "debug", From: "f"}},
		}
	}

	cases := []struct {
		name     string
		change   func(*domain.StreamSpec)
		expected bool
	}{
		{name: "identical", change: func(*domain.StreamSpec) {}, expected: true},
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
