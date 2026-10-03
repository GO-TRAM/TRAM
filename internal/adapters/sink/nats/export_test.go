package nats

import (
	"github.com/go-playground/validator/v10"

	"codeberg.org/eljoth/tram/internal/adapters/sink/base"
	"codeberg.org/eljoth/tram/internal/core/domain"
)

// Message is one message the sink would publish.
type Message struct {
	Subject string
	Event   *domain.Event
}

// NewWithoutConnection builds a sink that can group events but has no NATS
// connection to publish them.
func NewWithoutConnection(spec map[string]any, validator *validator.Validate) (*NATS, error) {
	b, err := base.New[Spec]("nats", "in", spec, validator)
	if err != nil {
		return nil, err
	}
	compiled, err := compileSubjects(b.Spec().Subject)
	if err != nil {
		return nil, err
	}
	return &NATS{Sink: b, subjects: compiled}, nil
}

func GroupEventsBySubjects(n *NATS, event *domain.Event) ([]Message, error) {
	messages, err := n.groupEventsBySubjects(event)
	if err != nil {
		return nil, err
	}
	out := make([]Message, len(messages))
	for i, m := range messages {
		out[i] = Message{Subject: m.subject, Event: m.event}
	}
	return out, nil
}
