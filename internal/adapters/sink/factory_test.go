package sink

import (
	"testing"

	"codeberg.org/eljoth/tram/internal/core/domain"
	"github.com/stretchr/testify/assert"
)

func TestFactoryUnknownType(t *testing.T) {
	_, err := NewFactory().New(domain.SinkSpec{Name: "s", Type: "unknown"})
	assert.ErrorIs(t, err, domain.ErrInvalidSinkType)
	assert.ErrorContains(t, err, `"unknown"`)
}
