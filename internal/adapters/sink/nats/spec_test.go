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
		name        string
		subject     map[string]any
		compression string
		expectErr   bool
	}{
		{
			name:        "s2 compression",
			subject:     map[string]any{"register": []any{"a.>"}, "logs": "'a.l'"},
			compression: "s2",
		},
		{
			name:        "no compression",
			subject:     map[string]any{"register": []any{"a.>"}, "logs": "'a.l'"},
			compression: "none",
		},
		{
			name:        "unknown compression",
			subject:     map[string]any{"register": []any{"a.>"}, "logs": "'a.l'"},
			compression: "zstd",
			expectErr:   true,
		},
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

			spec := map[string]any{
				"url":     "nats://localhost:4222",
				"stream":  "s",
				"subject": c.subject,
			}
			if c.compression != "" {
				spec["compression"] = c.compression
			}
			_, err := base.New[natssink.Spec]("nats", "src", spec, validator.New())
			if c.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
