package core

import (
	"errors"
	"fmt"

	"codeberg.org/eljoth/stream/internal/core/domain"
)

func (c *Core) buildStream(
	spec domain.StreamSpec,
) (*domain.Stream, error) {
	if err := spec.Validate(); err != nil {
		return nil, errors.Join(domain.ErrInvalidStreamSpec, err)
	}

	source, err := c.sourceFactory.New(spec.Source)
	if err != nil {
		return nil, errors.Join(domain.ErrInvalidStreamSpec, err)
	}

	stages := make(map[string]domain.Stage, len(spec.Stages))
	for _, stageSpec := range spec.Stages {
		stage, err := c.stageFactory.New(stageSpec)
		if err != nil {
			return nil, errors.Join(domain.ErrInvalidStreamSpec, err)
		}
		stages[stageSpec.Name] = stage
	}

	sinks := make(map[string]domain.Sink, len(spec.Sinks))
	for _, sinkSpec := range spec.Sinks {
		sink, err := c.sinkFactory.New(sinkSpec)
		if err != nil {
			return nil, errors.Join(domain.ErrInvalidStreamSpec, err)
		}
		sinks[sinkSpec.Name] = sink
	}

	return &domain.Stream{
		Spec:   spec,
		Source: source,
		Stages: stages,
		Sinks:  sinks,
	}, nil
}

func (c *Core) wireStream(stream *domain.Stream) error {
	for _, stage := range stream.Stages {
		if stage.From() == stream.Source.Name() {
			stream.Source.Subscribe(stage)
			continue
		}
		input, ok := stream.Stages[stage.From()]
		if !ok {
			return fmt.Errorf("%w: %s", domain.ErrStreamWireStages, stage.Name())
		}
		input.Subscribe(stage)
	}

	for _, sink := range stream.Sinks {
		if sink.From() == stream.Source.Name() {
			stream.Source.Subscribe(sink)
			continue
		}
		input, ok := stream.Stages[sink.From()]
		if !ok {
			return fmt.Errorf("%w: %s", domain.ErrStreamWireSinks, sink.Name())
		}
		input.Subscribe(sink)
	}

	return nil
}

func (c *Core) getStream(name string) (*domain.Stream, bool) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	stream, ok := c.streams[name]

	return stream, ok
}
