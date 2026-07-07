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

	for _, node := range tree.Map() {
		if node.Kind == KindSink && len(node.Children) != 0 {
			return ErrGraphSinkHasChildren
		}
	}

	return nil
}

func (s StreamSpec) Tree() (*Tree, error) {
	return NewTree(s)
}
