package domain

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
)

var streamNamePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)

func ValidateStreamName(name string) error {
	if !streamNamePattern.MatchString(name) {
		return fmt.Errorf("%w: %q must match %s", ErrInvalidStreamName, name, streamNamePattern)
	}
	return nil
}

func ParseSpec[T any](raw map[string]any, spec T) (T, error) {
	if raw == nil {
		return spec, nil
	}

	data, err := json.Marshal(raw)
	if err != nil {
		return spec, fmt.Errorf("failed to marshal spec: %w", err)
	}

	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&spec); err != nil {
		return spec, fmt.Errorf("failed to unmarshal spec: %w", err)
	}

	return spec, nil
}

type SourceSpec struct {
	Name string
	Type string
	Spec map[string]any
}
type StageSpec struct {
	Name string
	Type string
	From string
	Spec map[string]any
}
type SinkSpec struct {
	Name string
	Type string
	From string
	Spec map[string]any
}
type StreamSpec struct {
	Name   string
	Source SourceSpec
	Stages []StageSpec
	Sinks  []SinkSpec
}

func (s SourceSpec) Equal(o SourceSpec) bool {
	return s.Name == o.Name && s.Type == o.Type && componentSpecsEqual(s.Spec, o.Spec)
}

func (s StageSpec) Equal(o StageSpec) bool {
	return s.Name == o.Name && s.Type == o.Type && s.From == o.From && componentSpecsEqual(s.Spec, o.Spec)
}

func (s SinkSpec) Equal(o SinkSpec) bool {
	return s.Name == o.Name && s.Type == o.Type && s.From == o.From && componentSpecsEqual(s.Spec, o.Spec)
}

func (s StreamSpec) Equal(o StreamSpec) bool {
	return s.Name == o.Name &&
		s.Source.Equal(o.Source) &&
		equalByName(s.Stages, o.Stages, func(stage StageSpec) string { return stage.Name }, StageSpec.Equal) &&
		equalByName(s.Sinks, o.Sinks, func(sink SinkSpec) string { return sink.Name }, SinkSpec.Equal)
}

func equalByName[T any](a, b []T, name func(T) string, equal func(T, T) bool) bool {
	if len(a) != len(b) {
		return false
	}
	byName := make(map[string]T, len(b))
	for _, element := range b {
		byName[name(element)] = element
	}
	for _, element := range a {
		other, ok := byName[name(element)]
		if !ok || !equal(element, other) {
			return false
		}
		delete(byName, name(element))
	}
	return true
}

func componentSpecsEqual(a, b map[string]any) bool {
	if len(a) == 0 && len(b) == 0 {
		return true
	}
	encodedA, errA := json.Marshal(a)
	encodedB, errB := json.Marshal(b)
	if errA != nil || errB != nil {
		return reflect.DeepEqual(a, b)
	}
	return bytes.Equal(encodedA, encodedB)
}

type StreamSpecWatchEventType string

const (
	StreamSpecWatchEventTypeUpsert StreamSpecWatchEventType = "update"
	StreamSpecWatchEventTypeDelete StreamSpecWatchEventType = "delete"
)

type StreamSpecWatchEvent struct {
	StreamSpec StreamSpec
	EventType  StreamSpecWatchEventType
}
