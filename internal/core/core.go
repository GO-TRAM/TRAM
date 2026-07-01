package core

import (
	"codeberg.org/eljoth/stream/internal/core/domain"
	"codeberg.org/eljoth/stream/internal/ports"
)

type Core struct {
	streams       map[string]*domain.Stream
	sourceFactory ports.Factory[domain.SourceSpec, domain.Source]
	stageFactory  ports.Factory[domain.StageSpec, domain.Stage]
	sinkFactory   ports.Factory[domain.SinkSpec, domain.Sink]
}

func New(
	sourceFactory ports.Factory[domain.SourceSpec, domain.Source],
	stageFactory ports.Factory[domain.StageSpec, domain.Stage],
	sinkFactory ports.Factory[domain.SinkSpec, domain.Sink],
) *Core {
	return &Core{
		sourceFactory: sourceFactory,
		stageFactory:  stageFactory,
		sinkFactory:   sinkFactory,
	}
}

func (c *Core) UpsertStream(stream domain.StreamSpec) (*domain.StreamSpec, error) {
	c.BuildStream(domain.StreamSpec{})
	return nil, nil
}

func (c *Core) DeleteStream(name string) error {
	return nil
}

func (c *Core) GetStream(name string) (*domain.StreamSpec, error) {
	return nil, nil
}
