package nats

import (
	"errors"
	"log/slog"

	"github.com/go-playground/validator/v10"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"codeberg.org/eljoth/tram/internal/adapters/sink/base"
	"codeberg.org/eljoth/tram/internal/core/domain"
)

type Message struct {
	Subject string
	Event   *domain.Event
}

var errNoConnection = errors.New("sink has no NATS connection")

type noConnection struct {
	jetstream.JetStream
}

func (noConnection) PublishMsgAsync(*nats.Msg, ...jetstream.PublishOpt) (jetstream.PubAckFuture, error) {
	return nil, errNoConnection
}

// NewWithJetStream builds a sink that publishes through js.
func NewWithJetStream(spec map[string]any, validator *validator.Validate, js jetstream.JetStream) (*NATS, error) {
	n, err := NewWithoutConnection(spec, validator)
	if err != nil {
		return nil, err
	}
	n.jetstream = js
	return n, nil
}

func NewWithoutConnection(spec map[string]any, validator *validator.Validate) (*NATS, error) {
	b, err := base.New[Spec]("nats", "in", spec, validator)
	if err != nil {
		return nil, err
	}
	compiled, err := compileSubjects(b.Spec().Subject)
	if err != nil {
		return nil, err
	}
	return &NATS{
		Sink:      b,
		subjects:  compiled,
		logger:    slog.New(slog.DiscardHandler),
		jetstream: noConnection{},
	}, nil
}

func SubjectCovers(pattern, subject string) bool {
	return subjectCovers(pattern, subject)
}

type StreamUpdate struct {
	Changed        bool
	Subjects       []string
	ReplacedLimits []string
}

func MergeStreamConfig(existing jetstream.StreamConfig, spec *Spec) (jetstream.StreamConfig, StreamUpdate, error) {
	merged, update, err := mergeStreamConfig(existing, spec)
	return merged, StreamUpdate{
		Changed:        update.changed(),
		Subjects:       update.subjects,
		ReplacedLimits: update.replacedLimits,
	}, err
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
