package ports

import (
	"context"

	"codeberg.org/eljoth/tram/internal/core/domain"
)

type Core interface {
	UpsertStream(ctx context.Context, stream domain.StreamSpec) (*domain.StreamSpec, error)
	DeleteStream(ctx context.Context, name string) error
	GetStream(ctx context.Context, name string) (*domain.StreamSpec, error)
}
