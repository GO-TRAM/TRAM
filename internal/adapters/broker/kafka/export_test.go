// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package kafka

import "github.com/twmb/franz-go/pkg/kgo"

type (
	Tracker = tracker
	Entry   = entry
)

func NewTracker(mark func(*kgo.Record)) *Tracker { return newTracker(mark) }

func (t *tracker) Add(r *kgo.Record) *Entry             { return t.add(r) }
func (t *tracker) Finish(e *Entry)                      { t.finish(e) }
func (t *tracker) Tracked(e *Entry) bool                { return t.tracked(e) }
func (t *tracker) Revoke(partitions map[string][]int32) { t.revoke(partitions) }
