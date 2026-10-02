package stage

import (
	"testing"

	"codeberg.org/eljoth/tram/internal/core/domain"
	"github.com/stretchr/testify/assert"
)

func TestFactoryUnknownType(t *testing.T) {
	_, err := NewFactory().New(domain.StageSpec{Name: "s", Type: "unknown"})
	assert.ErrorIs(t, err, domain.ErrInvalidStageType)
	assert.ErrorContains(t, err, `"unknown"`)
}
