package ports

import "codeberg.org/eljoth/stream/internal/core/domain"

type Core interface {
	UpsertStream(stream domain.Stream) (*domain.Stream, error)
	DeleteStream(name string) error
	GetStream(name string) (*domain.Stream, error)
}
