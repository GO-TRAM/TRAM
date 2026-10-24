// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

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
	compiled, parts, err := compile(b.Spec())
	if err != nil {
		return nil, err
	}
	return &NATS{
		Sink:       b,
		subjects:   compiled,
		partitions: parts,
		logger:     slog.New(slog.DiscardHandler),
		jetstream:  noConnection{},
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
	return MergePartitionStreamConfig(existing, spec, 0)
}

// MergePartitionStreamConfig merges the stream of partition, which is the
// only stream without partitioning.
func MergePartitionStreamConfig(
	existing jetstream.StreamConfig,
	spec *Spec,
	partition int,
) (jetstream.StreamConfig, StreamUpdate, error) {
	merged, update, err := mergeStreamConfig(existing, spec, targetsOf(spec)[partition])
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

type Target struct {
	Name     string
	Subjects []string
}

// Targets returns the streams the sink publishes to.
func Targets(spec *Spec) []Target {
	var out []Target
	for _, t := range targetsOf(spec) {
		out = append(out, Target{Name: t.name, Subjects: t.subjects})
	}
	return out
}
