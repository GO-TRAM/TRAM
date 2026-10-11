// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package configsync

import (
	"reflect"

	"codeberg.org/eljoth/tram/internal/core/domain"
)

type SourceSpec struct {
	Name string         `json:"name"`
	Type string         `json:"type"`
	Spec map[string]any `json:"spec"`
}
type StageSpec struct {
	Name string         `json:"name"`
	Type string         `json:"type"`
	From string         `json:"from"`
	Spec map[string]any `json:"spec"`
}
type SinkSpec struct {
	Name string         `json:"name"`
	Type string         `json:"type"`
	From string         `json:"from"`
	Spec map[string]any `json:"spec"`
}
type StreamSpec struct {
	Name   string      `json:"name"`
	Source SourceSpec  `json:"source"`
	Stages []StageSpec `json:"stages"`
	Sinks  []SinkSpec  `json:"sinks"`
}

type Stream struct {
	Stream StreamSpec `json:"stream"`
}

func (s *Stream) Equals(other *Stream) bool {
	return reflect.DeepEqual(*s, *other)
}

func (s *Stream) AsDomain() *domain.StreamSpec {
	stages := make([]domain.StageSpec, 0, len(s.Stream.Stages))
	for _, stage := range s.Stream.Stages {
		stages = append(stages, domain.StageSpec{
			Name: stage.Name,
			Type: stage.Type,
			From: stage.From,
			Spec: stage.Spec,
		})
	}

	sinks := make([]domain.SinkSpec, 0, len(s.Stream.Sinks))
	for _, sink := range s.Stream.Sinks {
		sinks = append(sinks, domain.SinkSpec{
			Name: sink.Name,
			Type: sink.Type,
			From: sink.From,
			Spec: sink.Spec,
		})
	}

	return &domain.StreamSpec{
		Name: s.Stream.Name,
		Source: domain.SourceSpec{
			Name: s.Stream.Source.Name,
			Type: s.Stream.Source.Type,
			Spec: s.Stream.Source.Spec,
		},
		Stages: stages,
		Sinks:  sinks,
	}
}

func StreamFromDomain(spec domain.StreamSpec) *Stream {
	stages := make([]StageSpec, 0, len(spec.Stages))
	for _, stage := range spec.Stages {
		stages = append(stages, StageSpec{
			Name: stage.Name,
			Type: stage.Type,
			From: stage.From,
			Spec: stage.Spec,
		})
	}

	sinks := make([]SinkSpec, 0, len(spec.Sinks))
	for _, sink := range spec.Sinks {
		sinks = append(sinks, SinkSpec{
			Name: sink.Name,
			Type: sink.Type,
			From: sink.From,
			Spec: sink.Spec,
		})
	}

	return &Stream{
		Stream: StreamSpec{
			Name: spec.Name,
			Source: SourceSpec{
				Name: spec.Source.Name,
				Type: spec.Source.Type,
				Spec: spec.Source.Spec,
			},
			Stages: stages,
			Sinks:  sinks,
		},
	}
}
