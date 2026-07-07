package core

import (
	"errors"
	"fmt"

	"codeberg.org/eljoth/stream/internal/core/domain"
)

func (c *Core) buildStream(
	spec domain.StreamSpec,
) (*domain.Tree, error) {
	tree, err := domain.NewTree(spec)
	if err != nil {
		return nil, err
	}

	if !tree.IsTree() {
		return nil, domain.ErrGraphNotTree
	}

	if !tree.IsConnected() {
		return nil, domain.ErrGraphNotConnected
	}

	for _, node := range tree.LeafNodes() {
		if node.Kind != domain.KindSink {
			return nil, errors.Join(domain.ErrInvalidStreamSpec, domain.ErrGraphLeafNotSink)
		}
	}

	for _, node := range tree.Map() {
		if node.Kind == domain.KindSink && len(node.Children) != 0 {
			return nil, errors.Join(domain.ErrInvalidStreamSpec, domain.ErrGraphSinkHasChildren)
		}
	}

	source, err := c.sourceFactory.New(spec.Source)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("%w: %s", domain.ErrInvalidStreamSpec, spec.Source.Name), err)
	}
	if err := tree.Root().AddSource(source); err != nil {
		return nil, errors.Join(fmt.Errorf("%w: %s", domain.ErrInvalidStreamSpec, spec.Source.Name), err)
	}

	for _, stageSpec := range spec.Stages {
		stage, err := c.stageFactory.New(stageSpec)
		if err != nil {
			return nil, errors.Join(fmt.Errorf("%w: %s", domain.ErrInvalidStreamSpec, stageSpec.Name), err)
		}
		if err := tree.GetNode(stageSpec.Name).AddStage(stage); err != nil {
			return nil, errors.Join(fmt.Errorf("%w: %s", domain.ErrInvalidStreamSpec, stageSpec.Name), err)
		}
	}

	for _, sinkSpec := range spec.Sinks {
		sink, err := c.sinkFactory.New(sinkSpec)
		if err != nil {
			return nil, errors.Join(fmt.Errorf("%w: %s", domain.ErrInvalidStreamSpec, sinkSpec.Name), err)
		}
		if err := tree.GetNode(sinkSpec.Name).AddSink(sink); err != nil {
			return nil, errors.Join(fmt.Errorf("%w: %s", domain.ErrInvalidStreamSpec, sinkSpec.Name), err)
		}
	}

	nodes := tree.Map()
	for _, node := range nodes {
		if node.Parent == nil {
			continue
		}
		parent := node.Parent
		observer, err := parent.GetObserver()
		if err != nil {
			return nil, err
		}
		subscriber, err := node.GetSubscriber()
		if err != nil {
			return nil, err
		}
		if err := observer.Subscribe(subscriber); err != nil {
			return nil, err
		}
	}

	return tree, nil
}

func (c *Core) getStream(name string) (*domain.Tree, bool) {
	stream, ok := c.streams[name]

	return stream, ok
}
