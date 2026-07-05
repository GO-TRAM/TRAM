// internal/adapters/sink/base/sink.go
package base

type Sink struct {
	name string
	from string
}

func New(name, from string) *Sink {
	return &Sink{
		name: name,
		from: from,
	}
}

func (s *Sink) Name() string {
	return s.name
}

func (s *Sink) From() string {
	return s.from
}
