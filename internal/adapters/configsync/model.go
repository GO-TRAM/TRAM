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
	Name    string       `json:"name"`
	Source  SourceSpec   `json:"source"`
	Stages  []StageSpec  `json:"stages"`
	Sinks   []SinkSpec   `json:"sinks"`
	Metrics *MetricsSpec `json:"metrics,omitempty"`
}

// MetricsSpec and NodeMetricsSpec mirror domain.MetricsSpec; unset fields keep
// the defaults and are left out of written files.
type MetricsSpec struct {
	Records     *NodeMetricsSpec `json:"records,omitempty"`
	Duration    *NodeMetricsSpec `json:"duration,omitempty"`
	RequestSize *bool            `json:"request_size,omitempty"`
}

type NodeMetricsSpec struct {
	Source *bool `json:"source,omitempty"`
	Stages *bool `json:"stages,omitempty"`
	Sinks  *bool `json:"sinks,omitempty"`
}

func (m *MetricsSpec) asDomain() domain.MetricsSpec {
	if m == nil {
		return domain.MetricsSpec{}
	}
	return domain.MetricsSpec{
		Records:     m.Records.asDomain(),
		Duration:    m.Duration.asDomain(),
		RequestSize: m.RequestSize,
	}
}

func (n *NodeMetricsSpec) asDomain() domain.NodeMetrics {
	if n == nil {
		return domain.NodeMetrics{}
	}
	return domain.NodeMetrics{Source: n.Source, Stages: n.Stages, Sinks: n.Sinks}
}

func metricsFromDomain(m domain.MetricsSpec) *MetricsSpec {
	spec := &MetricsSpec{
		Records:     nodeMetricsFromDomain(m.Records),
		Duration:    nodeMetricsFromDomain(m.Duration),
		RequestSize: m.RequestSize,
	}
	if spec.Records == nil && spec.Duration == nil && spec.RequestSize == nil {
		return nil
	}
	return spec
}

func nodeMetricsFromDomain(n domain.NodeMetrics) *NodeMetricsSpec {
	if n.Source == nil && n.Stages == nil && n.Sinks == nil {
		return nil
	}
	return &NodeMetricsSpec{Source: n.Source, Stages: n.Stages, Sinks: n.Sinks}
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
		Stages:  stages,
		Sinks:   sinks,
		Metrics: s.Stream.Metrics.asDomain(),
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
			Stages:  stages,
			Sinks:   sinks,
			Metrics: metricsFromDomain(spec.Metrics),
		},
	}
}
