package ports

import (
	"context"

	"codeberg.org/eljoth/tram/internal/core/domain"
)

type Core interface {
	UpsertStream(ctx context.Context, stream domain.StreamSpec) (*domain.StreamSpec, error)
	DeleteStream(ctx context.Context, name string) error
	GetStream(ctx context.Context, name string) (*domain.StreamSpec, error)
	GetStreams(ctx context.Context) ([]domain.StreamSpec, error)
	// Watch sends every stream change to ch until ctx is done. Delivery is
	// synchronous: the caller must keep receiving until it cancels ctx, and
	// must not close ch.
	Watch(ctx context.Context, ch chan domain.StreamSpecWatchEvent) error
}
