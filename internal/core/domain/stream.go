package domain

type Stream struct {
	Name   string
	Source Source
	Stages []Stage
	Sinks  []Sink
	Graph  Graph
}

func (s *Stream) Validate() error {
	graph, err := s.buildGraph()
	if err != nil {
		return err
	}
	s.Graph = graph

	if !s.Graph.IsDAG() {
		return ErrGraphNotDAG
	}

	if !s.Graph.IsConnected() {
		return ErrGraphNotConnected
	}

	for _, node := range s.Graph.LeafNodes() {
		if node.Kind != KindSink {
			return ErrGraphLeafNotSink
		}
	}

	for _, node := range s.Graph {
		if node.Kind == KindSink && len(node.Children) != 0 {
			return ErrGraphSinkHasChildren
		}
	}

	return nil
}

func (s Stream) buildGraph() (Graph, error) {
	graph := make(map[string]*Node)
	graph[s.Source.Name] = &Node{
		Children: []Child{},
		Kind:     KindSource,
	}

	for _, stage := range s.Stages {
		if graph[stage.Name] != nil {
			return nil, ErrStageDuplicate
		}
		graph[stage.Name] = &Node{
			Children: []Child{},
			Kind:     KindStage,
		}
	}

	for _, stage := range s.Stages {
		from, ok := graph[stage.From]
		if !ok {
			return nil, ErrStageInvalidFrom
		}

		graph[stage.From].Children = append(from.Children, Child{Name: stage.Name, Kind: KindStage})
	}

	for _, sink := range s.Sinks {
		if graph[sink.Name] != nil {
			return nil, ErrSinkDuplicate
		}
		from, ok := graph[sink.From]
		if !ok {
			return nil, ErrSinkInvalidFrom
		}
		graph[sink.Name] = &Node{
			Children: []Child{},
			Kind:     KindSink,
		}
		graph[sink.From].Children = append(from.Children, Child{Name: sink.Name, Kind: KindSink})
	}
	return graph, nil
}
