// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package core

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"

	"go.opentelemetry.io/otel"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"codeberg.org/eljoth/tram/internal/core/domain"
	"codeberg.org/eljoth/tram/internal/ports"
)

// TestResult is the outcome of one stream test.
type TestResult struct {
	Name   string
	Passed bool
	// Problems describes per sink how the outcome differed from the
	// expectation.
	Problems []string
}

// RunStreamTests runs the spec's tests without touching a running stream: it
// builds its own tree in which the source is a test source that emits the
// test inputs and every sink is a capture sink, so no external system is
// contacted. Stages are built with the given factory and really process the
// events. Delivery is synchronous, so after an input returns, every sink has
// seen the event.
func RunStreamTests(
	ctx context.Context,
	spec domain.StreamSpec,
	stageFactory ports.Factory[domain.StageSpec, domain.Stage],
	logger *slog.Logger,
) ([]TestResult, error) {
	tree, err := newTree(spec)
	if err != nil {
		return nil, err
	}
	m, err := newMetrics(otel.GetMeterProvider())
	if err != nil {
		return nil, err
	}

	source := &testSource{}
	if err := tree.Root().AddSource(source); err != nil {
		return nil, err
	}
	captures := map[string]*captureSink{}
	c := &Core{
		logger:       logger,
		stageFactory: stageFactory,
		sinkFactory:  captureSinkFactory{captures: captures},
		metrics:      m,
	}
	if err := c.createReceivers(tree); err != nil {
		stopNodes(tree, false)
		return nil, err
	}
	if err := c.wire(tree, true); err != nil {
		stopNodes(tree, false)
		return nil, err
	}
	defer func() {
		if err := stopNodes(tree, false); err != nil {
			logger.ErrorContext(ctx, "Could not stop test components", slog.Any("error", err))
		}
	}()

	results := make([]TestResult, 0, len(spec.Tests))
	for _, test := range spec.Tests {
		results = append(results, runTest(ctx, spec, source, captures, test))
	}
	return results, nil
}

// runTest emits the test's input once and compares what each sink captured
// with the expectation. A sink without an expectation must receive nothing.
func runTest(
	ctx context.Context,
	spec domain.StreamSpec,
	source *testSource,
	captures map[string]*captureSink,
	test domain.StreamTest,
) TestResult {
	result := TestResult{Name: test.Name}

	event, err := parseEvent(test.Input)
	if err != nil {
		result.Problems = append(result.Problems, "invalid input: "+err.Error())
		return result
	}
	for _, capture := range captures {
		capture.reset()
	}

	emitErr := source.emit(ctx, event)

	expectReject := false
	for _, expected := range test.Expect {
		if keyword, ok := expected.(string); ok && keyword == "reject" {
			expectReject = true
		}
	}
	if expectReject {
		if emitErr == nil {
			result.Problems = append(result.Problems, "expected the event to be rejected, but it was accepted")
		} else {
			result.Passed = true
		}
		return result
	}
	if emitErr != nil {
		result.Problems = append(result.Problems, fmt.Sprintf("event rejected unexpectedly: %v", emitErr))
		return result
	}

	for _, sinkSpec := range spec.Sinks {
		capture := captures[sinkSpec.Name]
		expected, declared := test.Expect[sinkSpec.Name]
		if keyword, isKeyword := expected.(string); declared && isKeyword && keyword == "drop" {
			declared = false
		}
		if !declared {
			if len(capture.events) > 0 {
				result.Problems = append(result.Problems,
					fmt.Sprintf("sink %s: received %s, expected drop", sinkSpec.Name, describeEvents(capture.events)))
			}
			continue
		}
		expectedEvent, err := parseEvent(asMap(expected))
		if err != nil {
			result.Problems = append(result.Problems, fmt.Sprintf("sink %s: invalid expectation: %v", sinkSpec.Name, err))
			continue
		}
		if !capture.matches(expectedEvent) {
			result.Problems = append(result.Problems,
				fmt.Sprintf("sink %s: received %s", sinkSpec.Name, describeEvents(capture.events)))
		}
	}
	if len(result.Problems) == 0 {
		result.Passed = true
	}
	return result
}

// parseEvent turns an OTLP JSON object with one of the keys logs, metrics or
// traces into an event.
func parseEvent(raw map[string]any) (*domain.Event, error) {
	event := &domain.Event{}
	if raw == nil {
		return event, nil
	}
	for signal, value := range raw {
		data, err := json.Marshal(value)
		if err != nil {
			return nil, err
		}
		switch signal {
		case "logs":
			event.Logs = &logspb.LogsData{}
			if err := protojson.Unmarshal(data, event.Logs); err != nil {
				return nil, fmt.Errorf("logs: %w", err)
			}
		case "metrics":
			event.Metrics = &metricspb.MetricsData{}
			if err := protojson.Unmarshal(data, event.Metrics); err != nil {
				return nil, fmt.Errorf("metrics: %w", err)
			}
		case "traces":
			event.Traces = &tracepb.TracesData{}
			if err := protojson.Unmarshal(data, event.Traces); err != nil {
				return nil, fmt.Errorf("traces: %w", err)
			}
		default:
			return nil, fmt.Errorf("unknown signal %q, want logs, metrics or traces", signal)
		}
	}
	return event, nil
}

func asMap(value any) map[string]any {
	if m, ok := value.(map[string]any); ok {
		return m
	}
	return nil
}

func describeEvents(events []*domain.Event) string {
	if len(events) == 0 {
		return "nothing"
	}
	if len(events) == 1 {
		return "one event"
	}
	return fmt.Sprintf("%d events", len(events))
}

// testSource is the source of a test tree: it emits the test inputs to its
// subscribers, synchronously.
type testSource struct {
	mu   sync.Mutex
	subs []domain.Subscriber
}

func (s *testSource) Name() string { return "test" }
func (s *testSource) Stop() error  { return nil }
func (s *testSource) Subscribe(sub domain.Subscriber) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.subs = append(s.subs, sub)
	return nil
}
func (s *testSource) SetSubscribers(subs []domain.Subscriber) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.subs = subs
}
func (s *testSource) Run(ctx context.Context) error {
	<-ctx.Done()
	return nil
}

func (s *testSource) emit(ctx context.Context, event *domain.Event) error {
	s.mu.Lock()
	subs := make([]domain.Subscriber, len(s.subs))
	copy(subs, s.subs)
	s.mu.Unlock()
	for _, sub := range subs {
		if err := sub.Notify(ctx, event); err != nil {
			return err
		}
	}
	return nil
}

// captureSink records the events it receives.
type captureSink struct {
	name   string
	mu     sync.Mutex
	events []*domain.Event
}

func (s *captureSink) Name() string { return s.name }
func (s *captureSink) From() string { return "test" }
func (s *captureSink) Stop() error  { return nil }
func (s *captureSink) Notify(_ context.Context, event *domain.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, event)
	return nil
}

func (s *captureSink) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = nil
}

// matches reports whether exactly one event was captured and it equals the
// expected one.
func (s *captureSink) matches(expected *domain.Event) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.events) != 1 {
		return false
	}
	got := s.events[0]
	return proto.Equal(got.Logs, expected.Logs) &&
		proto.Equal(got.Metrics, expected.Metrics) &&
		proto.Equal(got.Traces, expected.Traces)
}

// captureSinkFactory replaces every sink with a capture sink, so tests never
// contact an external system.
type captureSinkFactory struct {
	captures map[string]*captureSink
}

func (f captureSinkFactory) New(spec domain.SinkSpec) (domain.Sink, error) {
	sink := &captureSink{name: spec.Name}
	f.captures[spec.Name] = sink
	return sink, nil
}
