package domain

import (
	"encoding/json"
	"fmt"
)

func ParseSpec[T any](raw map[string]any, spec T) (T, error) {
	if raw == nil {
		return spec, nil
	}

	data, err := json.Marshal(raw)
	if err != nil {
		return spec, fmt.Errorf("failed to marshal spec: %w", err)
	}

	if err := json.Unmarshal(data, &spec); err != nil {
		return spec, fmt.Errorf("failed to unmarshal spec: %w", err)
	}

	return spec, nil
}

type SourceSpec struct {
	Name string
	Type string
	Spec map[string]any
}
type StageSpec struct {
	Name string
	Type string
	From string
	Spec map[string]any
}
type SinkSpec struct {
	Name string
	Type string
	From string
	Spec map[string]any
}
type StreamSpec struct {
	Name   string
	Source SourceSpec
	Stages []StageSpec
	Sinks  []SinkSpec
}

func (s *StreamSpec) Validate() error {
	tree, err := s.Tree()
	if err != nil {
		return err
	}

	if !tree.IsTree() {
		return ErrGraphNotTree
	}

	if !tree.IsConnected() {
		return ErrGraphNotConnected
	}

	for _, node := range tree.LeafNodes() {
		if node.Kind != KindSink {
			return ErrGraphLeafNotSink
		}
	}

	for _, node := range tree {
		if node.Kind == KindSink && len(node.Children) != 0 {
			return ErrGraphSinkHasChildren
		}
	}

	return nil
}

func (s StreamSpec) Tree() (Tree, error) {
	tree := make(map[string]*Node)
	tree[s.Source.Name] = &Node{
		Children: []*Node{},
		Kind:     KindSource,
		Name:     s.Source.Name,
	}

	for _, stage := range s.Stages {
		if _, ok := tree[stage.Name]; ok {
			return nil, ErrStageDuplicate
		}
		tree[stage.Name] = &Node{
			Children: []*Node{},
			Kind:     KindStage,
			Name:     stage.Name,
		}
	}

	for _, stage := range s.Stages {
		from, ok := tree[stage.From]
		if !ok {
			return nil, ErrStageInvalidFrom
		}

		node := tree[stage.Name]
		tree[stage.From].Children = append(from.Children, node)
	}

	for _, sink := range s.Sinks {
		if _, ok := tree[sink.Name]; ok {
			return nil, ErrSinkDuplicate
		}
		from, ok := tree[sink.From]
		if !ok {
			return nil, ErrSinkInvalidFrom
		}
		node := &Node{
			Children: []*Node{},
			Kind:     KindSink,
			Name:     sink.Name,
		}
		tree[sink.Name] = node
		from.Children = append(from.Children, node)
	}
	return tree, nil
}
