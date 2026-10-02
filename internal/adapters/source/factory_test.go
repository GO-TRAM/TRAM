package source

import (
	"testing"

	"codeberg.org/eljoth/tram/internal/core/domain"
	"github.com/stretchr/testify/assert"
)

func TestFactoryUnknownType(t *testing.T) {
	_, err := NewFactory().New(domain.SourceSpec{Name: "s", Type: "unknown"})
	assert.ErrorIs(t, err, domain.ErrInvalidSourceType)
	assert.ErrorContains(t, err, `"unknown"`)
}
