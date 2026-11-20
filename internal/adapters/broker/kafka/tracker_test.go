// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package kafka_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/twmb/franz-go/pkg/kgo"

	kafkadriver "codeberg.org/tram/tram/internal/adapters/broker/kafka"
)

func TestTrackerMarksOnlyAfterEveryRecordBefore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		finish []int // indexes of the records, in the order they finish
		want   []int64
	}{
		{name: "in order", finish: []int{0, 1, 2}, want: []int64{0, 1, 2}},
		{name: "reversed", finish: []int{2, 1, 0}, want: []int64{2}},
		{name: "gap closed last", finish: []int{0, 2, 1}, want: []int64{0, 2}},
		{name: "head still open", finish: []int{1, 2}, want: nil},
		{name: "finished twice", finish: []int{0, 0, 1}, want: []int64{0, 1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var marked []int64
			tr := kafkadriver.NewTracker(func(r *kgo.Record) { marked = append(marked, r.Offset) })
			entries := make([]*kafkadriver.Entry, 3)
			for i := range entries {
				entries[i] = tr.Add(&kgo.Record{Topic: "t", Partition: 0, Offset: int64(i)})
			}
			for _, i := range tt.finish {
				tr.Finish(entries[i])
			}
			assert.Equal(t, tt.want, marked)
		})
	}
}

func TestTrackerKeepsPartitionsApart(t *testing.T) {
	t.Parallel()

	var marked []string
	tr := kafkadriver.NewTracker(func(r *kgo.Record) { marked = append(marked, r.Topic) })
	a := tr.Add(&kgo.Record{Topic: "a", Partition: 0, Offset: 5})
	tr.Add(&kgo.Record{Topic: "a", Partition: 1, Offset: 0})
	tr.Finish(a)
	assert.Equal(t, []string{"a"}, marked, "an open record of another partition does not hold it back")
}

func TestTrackerForgetsRevokedPartitions(t *testing.T) {
	t.Parallel()

	var marked int
	tr := kafkadriver.NewTracker(func(*kgo.Record) { marked++ })
	e := tr.Add(&kgo.Record{Topic: "t", Partition: 3, Offset: 0})
	tr.Revoke(map[string][]int32{"t": {3}})
	assert.False(t, tr.Tracked(e))
	tr.Finish(e)
	assert.Zero(t, marked, "the partition's new owner commits it")

	again := tr.Add(&kgo.Record{Topic: "t", Partition: 3, Offset: 0})
	assert.True(t, tr.Tracked(again), "a partition assigned again is tracked anew")
	assert.False(t, tr.Tracked(e))
}
