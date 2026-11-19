// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package domain

// StreamPlanAction describes what an upsert of a stream spec would do.
type StreamPlanAction string

const (
	// StreamPlanActionNoop means the stream is unchanged, nothing would
	// happen.
	StreamPlanActionNoop StreamPlanAction = "noop"
	// StreamPlanActionHotSwap means the source is unchanged and keeps running;
	// its stages and sinks are replaced below it.
	StreamPlanActionHotSwap StreamPlanAction = "hot_swap"
	// StreamPlanActionRebuild means the stream is new, stopped, or its source
	// changed: it is stopped and rebuilt completely.
	StreamPlanActionRebuild StreamPlanAction = "rebuild"
)

// StreamPlan is the plan of an upsert, computed by a dry run: what applying
// the spec would do, without changing the running stream. Node names are
// unique within a stream.
type StreamPlan struct {
	Stream string
	Action StreamPlanAction
	// SourceReplaced is true when the action is a rebuild of an existing
	// stream with another source.
	SourceReplaced bool
	// CreatedNodes are the stages and sinks that would be created, by name.
	CreatedNodes []string
	// StoppedNodes are the stages and sinks that would be stopped, by name.
	StoppedNodes []string
}
