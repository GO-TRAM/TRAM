package api

import (
	"errors"
	"fmt"
	"testing"

	"codeberg.org/eljoth/tram/internal/core/domain"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestToStatus(t *testing.T) {
	cases := []struct {
		name     string
		err      error
		expected codes.Code
	}{
		{name: "not found", err: domain.ErrStreamNotFound, expected: codes.NotFound},
		{name: "build failed", err: errors.Join(domain.ErrStreamBuildFailed, domain.ErrGraphNotTree), expected: codes.InvalidArgument},
		{name: "core closed", err: fmt.Errorf("upsert: %w", domain.ErrCoreClosed), expected: codes.Unavailable},
		{name: "unknown", err: errors.New("boom"), expected: codes.Internal},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.expected, status.Code(toStatus(c.err)))
		})
	}
}
