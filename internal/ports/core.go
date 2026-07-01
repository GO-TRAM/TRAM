package ports

import "codeberg.org/eljoth/stream/internal/core/domain"

type Core interface {
	UpsertStream(stream domain.StreamSpec) (*domain.StreamSpec, error)
	DeleteStream(name string) error
	GetStream(name string) (*domain.StreamSpec, error)
}
