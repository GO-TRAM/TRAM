package core

import "codeberg.org/eljoth/stream/internal/core/domain"

type Core struct {
	streams map[string]*domain.Stream
}

func New() *Core {
	return &Core{}
}

func (c *Core) UpsertStream(stream domain.Stream) (*domain.Stream, error) {
	return nil, nil
}

func (c *Core) DeleteStream(name string) error {
	return nil
}

func (c *Core) GetStream(name string) (*domain.Stream, error) {
	return nil, nil
}
