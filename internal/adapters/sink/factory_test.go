package sink_test

import (
	"log/slog"

	"codeberg.org/eljoth/tram/internal/adapters/sink"

	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"codeberg.org/eljoth/tram/internal/core/domain"
)

func TestFactoryUnknownType(t *testing.T) {
	t.Parallel()

	_, err := sink.NewFactory(slog.New(slog.DiscardHandler)).New(domain.SinkSpec{Name: "s", Type: "unknown"})
	require.ErrorIs(t, err, domain.ErrInvalidSinkType)
	assert.ErrorContains(t, err, `"unknown"`)
}
