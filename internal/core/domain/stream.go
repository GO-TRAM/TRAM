package domain

type Stream struct {
	Spec   StreamSpec
	Source Source
	Stages []Stage
	Sinks  []Sink
}
