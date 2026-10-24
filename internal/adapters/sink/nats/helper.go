// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package nats

import (
	"fmt"
	"strings"

	"github.com/google/cel-go/cel"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"

	"codeberg.org/eljoth/tram/internal/adapters/natspartition"
	"codeberg.org/eljoth/tram/internal/adapters/sink/celentry"
	"codeberg.org/eljoth/tram/internal/core/domain"

	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	tracespb "go.opentelemetry.io/proto/otlp/trace/v1"
)

type message struct {
	subject string
	event   *domain.Event
}

func (n *NATS) groupEventsBySubjects(event *domain.Event) ([]message, error) {
	var messages []message
	var keys subjects
	if n.partitions != nil {
		keys = n.partitions.keys
	}

	if event.Logs != nil && n.Spec().Subject.Logs != "" {
		parts, err := domain.PartitionLogs(event.Logs, celentry.LogKey(n.logs, subjectOf(n.partitioner(keys.logs))))
		if err != nil {
			return nil, fmt.Errorf("logs: %w", err)
		}
		messages = appendMessages(messages, parts, func(d *logspb.LogsData) *domain.Event {
			return &domain.Event{Logs: d}
		})
	}

	if event.Metrics != nil && n.Spec().Subject.Metrics != "" {
		parts, err := domain.PartitionMetrics(
			event.Metrics,
			celentry.MetricKey(n.metrics, subjectOf(n.partitioner(keys.metrics))),
		)
		if err != nil {
			return nil, fmt.Errorf("metrics: %w", err)
		}
		messages = appendMessages(messages, parts, func(d *metricspb.MetricsData) *domain.Event {
			return &domain.Event{Metrics: d}
		})
	}

	if event.Traces != nil && n.Spec().Subject.Traces != "" {
		parts, err := domain.PartitionSpans(
			event.Traces,
			celentry.SpanKey(n.traces, subjectOf(n.partitioner(keys.traces))),
		)
		if err != nil {
			return nil, fmt.Errorf("traces: %w", err)
		}
		messages = appendMessages(messages, parts, func(d *tracespb.TracesData) *domain.Event {
			return &domain.Event{Traces: d}
		})
	}

	return messages, nil
}

func appendMessages[D any](messages []message, parts []domain.Part[D], event func(D) *domain.Event) []message {
	for _, part := range parts {
		messages = append(messages, message{subject: part.Key, event: event(part.Data)})
	}
	return messages
}

// subjectOf resolves the subject of an entry. With part, it first sets the
// entry's partition and then checks that the subject belongs to it.
func subjectOf(part *partitioner) celentry.Resolve {
	return func(program cel.Program, vars *celentry.Vars) (string, error) {
		return resolveSubject(program, part, vars)
	}
}

func resolveSubject(program cel.Program, part *partitioner, vars *celentry.Vars) (string, error) {
	if part == nil {
		return evalSubject(program, vars)
	}
	key, err := celentry.String(part.key, vars)
	if err != nil {
		return "", fmt.Errorf("partition key: %w", err)
	}
	partition := natspartition.Of(key, part.count)
	vars.Set(partitionVariable, int64(partition))
	subject, err := evalSubject(program, vars)
	if err != nil {
		return "", err
	}
	return subject, part.check(subject, partition)
}

func evalSubject(program cel.Program, vars *celentry.Vars) (string, error) {
	subject, err := celentry.String(program, vars)
	if err != nil {
		return "", err
	}
	if err = validSubject(subject); err != nil {
		return "", err
	}
	return subject, nil
}

// partitioner checks the subjects of one signal of an event against the
// partitions of their entries.
type partitioner struct {
	*partitions

	key cel.Program
	// checked holds the partition each subject was found in, so a subject is
	// checked once per event.
	checked map[string]int
}

// partitioner returns nil without partitioning.
func (n *NATS) partitioner(key cel.Program) *partitioner {
	if n.partitions == nil {
		return nil
	}
	return &partitioner{partitions: n.partitions, key: key, checked: map[string]int{}}
}

// check returns an error if subject is not stored in the stream of partition.
// JetStream picks the stream by subject, so a subject expression that ignores
// the partition would otherwise put entries into another partition's stream.
func (p *partitioner) check(subject string, partition int) error {
	if checked, ok := p.checked[subject]; ok && checked == partition {
		return nil
	}
	if !covered(p.registers[partition], []string{subject}) {
		return fmt.Errorf("subject %q is not stored in partition %d, whose stream holds %v",
			subject, partition, p.registers[partition])
	}
	p.checked[subject] = partition
	return nil
}

func validSubject(subject string) error {
	for token := range strings.SplitSeq(subject, ".") {
		switch {
		case token == "":
			return fmt.Errorf("subject %q has an empty token", subject)
		case token == "*" || token == ">":
			return fmt.Errorf("subject %q contains a wildcard", subject)
		case strings.ContainsAny(token, " \t\r\n"):
			return fmt.Errorf("subject %q contains whitespace", subject)
		}
	}
	return nil
}
