package domain

type Sink interface {
	Emitter
}

type SinkSpec struct {
	Name string
	Type string
	From string
}
