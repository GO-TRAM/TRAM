package source_test

import (
	"log/slog"

	"codeberg.org/eljoth/tram/internal/adapters/source"

	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"codeberg.org/eljoth/tram/internal/core/domain"
)

func TestFactoryUnknownType(t *testing.T) {
	t.Parallel()

	_, err := source.NewFactory(slog.New(slog.DiscardHandler)).New(domain.SourceSpec{Name: "s", Type: "unknown"})
	require.ErrorIs(t, err, domain.ErrInvalidSourceType)
	assert.ErrorContains(t, err, `"unknown"`)
}
