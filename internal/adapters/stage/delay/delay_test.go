package delay

import (
	"context"
	"testing"
	"time"

	"codeberg.org/eljoth/tram/internal/core/domain"
	"github.com/go-playground/validator/v10"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNotifyReturnsEarly(t *testing.T) {
	cases := []struct {
		name      string
		interrupt func(d *Delay, cancel context.CancelFunc)
		expected  error
	}{
		{
			name:      "context cancelled",
			interrupt: func(_ *Delay, cancel context.CancelFunc) { cancel() },
			expected:  context.Canceled,
		},
		{
			name:      "stage stopped",
			interrupt: func(d *Delay, _ context.CancelFunc) { _ = d.Stop() },
			expected:  ErrDelayStopped,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			d, err := New(domain.StageSpec{Name: "delay", Spec: map[string]any{"delay": "1h"}}, validator.New())
			require.NoError(t, err)

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			done := make(chan error, 1)
			go func() { done <- d.Notify(ctx, &domain.Event{}) }()
			c.interrupt(d, cancel)

			select {
			case err := <-done:
				assert.ErrorIs(t, err, c.expected)
			case <-time.After(time.Second):
				t.Fatal("Notify did not return")
			}
		})
	}
}

func TestStopIsIdempotent(t *testing.T) {
	d, err := New(domain.StageSpec{Name: "delay", Spec: map[string]any{"delay": "1h"}}, validator.New())
	require.NoError(t, err)

	assert.NoError(t, d.Stop())
	assert.NoError(t, d.Stop())
}
