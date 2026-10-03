package domain

import (
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"slices"
)

// streamNamePattern allows letters, digits, dots, dashes and underscores, and
// requires a letter or digit first. Drivers use the name as an identifier
// outside the core, e.g. the file sync as a file name, so it must not contain
// path separators or be a relative path like "..". It covers the names the
// Kubernetes operator builds from namespace and resource name.
var streamNamePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)

// ValidateStreamName reports whether name can be used as a stream name.
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

	if err = json.Unmarshal(data, &spec); err != nil {
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

// The Equal methods report whether two specs describe the same component or
// stream. Missing and empty lists and component specs count as equal, because
// the API, the file sync and the Kubernetes operator represent them
// differently.

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
		slices.EqualFunc(s.Stages, o.Stages, StageSpec.Equal) &&
		slices.EqualFunc(s.Sinks, o.Sinks, SinkSpec.Equal)
}

func componentSpecsEqual(a, b map[string]any) bool {
	if len(a) == 0 && len(b) == 0 {
		return true
	}
	return reflect.DeepEqual(a, b)
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
