package generator_test

import (
	"testing"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/stretchr/testify/require"

	"codeberg.org/eljoth/tram/internal/adapters/source/generator"
	"codeberg.org/eljoth/tram/internal/core/domain"
)

func TestRunReturnsAfterStop(t *testing.T) {
	t.Parallel()

	g, err := generator.New(domain.SourceSpec{
		Name: "generator",
		Spec: map[string]any{"interval": "1h"},
	}, validator.New())
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() { done <- g.Run(t.Context()) }()

	require.NoError(t, g.Stop())

	select {
	case runErr := <-done:
		require.NoError(t, runErr)
	case <-time.After(time.Second):
		t.Fatal("Run did not return after Stop")
	}
}
