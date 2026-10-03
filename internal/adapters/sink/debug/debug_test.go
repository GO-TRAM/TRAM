package debug_test

import (
	"log/slog"

	"codeberg.org/eljoth/tram/internal/adapters/sink/debug"

	"testing"

	"github.com/go-playground/validator/v10"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"codeberg.org/eljoth/tram/internal/core/domain"
)

func TestNotifyWithoutPath(t *testing.T) {
	t.Parallel()

	d, err := debug.New(domain.SinkSpec{Name: "debug"}, validator.New(), slog.New(slog.DiscardHandler))
	require.NoError(t, err)

	assert.NotPanics(t, func() {
		assert.NoError(t, d.Notify(t.Context(), &domain.Event{}))
	})
}
