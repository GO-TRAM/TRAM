package generator

import (
	"testing"
	"time"

	"codeberg.org/eljoth/tram/internal/core/domain"
	"github.com/go-playground/validator/v10"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRunReturnsAfterStop(t *testing.T) {
	g, err := New(domain.SourceSpec{
		Name: "generator",
		Spec: map[string]any{"interval": "1h"},
	}, validator.New())
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() { done <- g.Run(t.Context()) }()

	require.NoError(t, g.Stop())

	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("Run did not return after Stop")
	}
}
