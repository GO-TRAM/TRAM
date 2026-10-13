// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package domain

// MetricsSpec chooses the metrics a stream records. Every node is a series of
// its own per metric, so streams with many stages, or many alike streams, can
// leave out what they do not need. Unset fields keep their defaults.
type MetricsSpec struct {
	// Records covers tram.node.records and tram.node.records.out.
	Records NodeMetrics
	// Duration covers tram.node.duration. A node's duration includes the
	// nodes below it, so the time of the stages is the source's duration
	// minus the sinks'.
	Duration NodeMetrics
	// RequestSize covers tram.source.request.size, recorded by sources only.
	// Defaults to true.
	RequestSize *bool
}

// NodeMetrics enables a metric per kind of node. Sources and sinks default to
// true, stages to false.
type NodeMetrics struct {
	Source *bool
	Stages *bool
	Sinks  *bool
}

// Enabled reports whether nodes of the kind record the metric.
func (m NodeMetrics) Enabled(kind NodeKind) bool {
	switch kind {
	case KindSource:
		return valueOr(m.Source, true)
	case KindStage:
		return valueOr(m.Stages, false)
	case KindSink:
		return valueOr(m.Sinks, true)
	default:
		return false
	}
}

// RequestSizeEnabled reports whether sources record the size of requests.
func (m MetricsSpec) RequestSizeEnabled() bool {
	return valueOr(m.RequestSize, true)
}

// Equal compares the effective settings, so an unset field equals its default.
func (m MetricsSpec) Equal(o MetricsSpec) bool {
	return m.Records.Equal(o.Records) && m.Duration.Equal(o.Duration) &&
		m.RequestSizeEnabled() == o.RequestSizeEnabled()
}

// Equal compares the effective settings, so an unset field equals its default.
func (m NodeMetrics) Equal(o NodeMetrics) bool {
	for _, kind := range []NodeKind{KindSource, KindStage, KindSink} {
		if m.Enabled(kind) != o.Enabled(kind) {
			return false
		}
	}
	return true
}

func valueOr(value *bool, fallback bool) bool {
	if value == nil {
		return fallback
	}
	return *value
}
