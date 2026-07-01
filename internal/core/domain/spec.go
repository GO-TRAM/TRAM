package domain

type SourceSpec struct {
	Name string
	Type string
}
type StageSpec struct {
	Name string
	Type string
	From string
}
type SinkSpec struct {
	Name string
	Type string
	From string
}
type StreamSpec struct {
	Name   string
	Source SourceSpec
	Stages []StageSpec
	Sinks  []SinkSpec
}

func (s *StreamSpec) Validate() error {
	graph, err := s.Graph()
	if err != nil {
		return err
	}

	if !graph.IsDAG() {
		return ErrGraphNotDAG
	}

	if !graph.IsConnected() {
		return ErrGraphNotConnected
	}

	for _, node := range graph.LeafNodes() {
		if node.Kind != KindSink {
			return ErrGraphLeafNotSink
		}
	}

	for _, node := range graph {
		if node.Kind == KindSink && len(node.Children) != 0 {
			return ErrGraphSinkHasChildren
		}
	}

	return nil
}

func (s StreamSpec) Graph() (Graph, error) {
	graph := make(map[string]*Node)
	graph[s.Source.Name] = &Node{
		Children: []*Node{},
		Kind:     KindSource,
		Name:     s.Source.Name,
	}

	for _, stage := range s.Stages {
		if _, ok := graph[stage.Name]; ok {
			return nil, ErrStageDuplicate
		}
		graph[stage.Name] = &Node{
			Children: []*Node{},
			Kind:     KindStage,
			Name:     stage.Name,
		}
	}

	for _, stage := range s.Stages {
		from, ok := graph[stage.From]
		if !ok {
			return nil, ErrStageInvalidFrom
		}

		node := graph[stage.Name]
		graph[stage.From].Children = append(from.Children, node)
	}

	for _, sink := range s.Sinks {
		if _, ok := graph[sink.Name]; ok {
			return nil, ErrSinkDuplicate
		}
		from, ok := graph[sink.From]
		if !ok {
			return nil, ErrSinkInvalidFrom
		}
		node := &Node{
			Children: []*Node{},
			Kind:     KindSink,
			Name:     sink.Name,
		}
		graph[sink.Name] = node
		from.Children = append(from.Children, node)
	}
	return graph, nil
}
