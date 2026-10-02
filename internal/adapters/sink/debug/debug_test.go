package debug

import (
	"testing"

	"codeberg.org/eljoth/tram/internal/core/domain"
	"github.com/go-playground/validator/v10"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNotifyWithoutPath(t *testing.T) {
	d, err := New(domain.SinkSpec{Name: "debug"}, validator.New())
	require.NoError(t, err)

	assert.NotPanics(t, func() {
		assert.NoError(t, d.Notify(t.Context(), &domain.Event{}))
	})
}
