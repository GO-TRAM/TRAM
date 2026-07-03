package domain

type Stream struct {
	Spec   StreamSpec
	Source Source
	Stages map[string]Stage
	Sinks  map[string]Sink
}
