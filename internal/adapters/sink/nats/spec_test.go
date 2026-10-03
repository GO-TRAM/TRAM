package nats_test

import (
	"testing"

	natssink "codeberg.org/eljoth/tram/internal/adapters/sink/nats"

	"github.com/go-playground/validator/v10"
	"github.com/stretchr/testify/assert"

	"codeberg.org/eljoth/tram/internal/adapters/sink/base"
)

func TestSpecValidation(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		subject   map[string]any
		expectErr bool
	}{
		{
			name:    "all signals",
			subject: map[string]any{"register": []any{"a.>"}, "logs": "'a.l'", "metrics": "'a.m'", "traces": "'a.t'"},
		},
		{
			name:    "single signal",
			subject: map[string]any{"register": []any{"a.>"}, "metrics": "'a.m'"},
		},
		{
			name:      "no signal",
			subject:   map[string]any{"register": []any{"a.>"}},
			expectErr: true,
		},
		{
			name:      "no register",
			subject:   map[string]any{"logs": "'a.l'"},
			expectErr: true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			_, err := base.New[natssink.Spec]("nats", "src", map[string]any{
				"url":     "nats://localhost:4222",
				"stream":  "s",
				"subject": c.subject,
			}, validator.New())
			if c.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
