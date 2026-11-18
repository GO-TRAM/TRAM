// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package kafka

import (
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
)

// tracker keeps the records handed out per partition, in offset order. Kafka
// commits a position per partition, not single messages, so a record's offset
// may only be committed once it and every record before it are done; workers
// finish them in any order.
type tracker struct {
	mu    sync.Mutex
	parts map[topicPartition]*partitionState
	// mark marks a record's offset for the next commit.
	mark func(*kgo.Record)
}

type topicPartition struct {
	topic     string
	partition int32
}

type partitionState struct {
	entries []*entry
}

// entry is one record handed out. done is set once its delivery ended.
type entry struct {
	record *kgo.Record
	part   *partitionState
	done   bool
}

func newTracker(mark func(*kgo.Record)) *tracker {
	return &tracker{parts: map[topicPartition]*partitionState{}, mark: mark}
}

// add tracks a record received from the broker.
func (t *tracker) add(r *kgo.Record) *entry {
	t.mu.Lock()
	defer t.mu.Unlock()
	key := topicPartition{r.Topic, r.Partition}
	part, ok := t.parts[key]
	if !ok {
		part = &partitionState{}
		t.parts[key] = part
	}
	e := &entry{record: r, part: part}
	part.entries = append(part.entries, e)
	return e
}

// finish ends e and marks the offset after the longest run of finished
// records at the head of its partition.
func (t *tracker) finish(e *entry) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if e.done || !t.trackedLocked(e) {
		return
	}
	e.done = true
	part := e.part
	var last *kgo.Record
	for len(part.entries) > 0 && part.entries[0].done {
		last = part.entries[0].record
		part.entries = part.entries[1:]
	}
	if last != nil {
		t.mark(last)
	}
}

// tracked reports whether e's partition is still assigned to this subscriber.
func (t *tracker) tracked(e *entry) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.trackedLocked(e)
}

func (t *tracker) trackedLocked(e *entry) bool {
	return t.parts[topicPartition{e.record.Topic, e.record.Partition}] == e.part
}

// revoke forgets partitions taken from this subscriber. Their open records
// are read again by the partitions' new owner.
func (t *tracker) revoke(partitions map[string][]int32) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for topic, ps := range partitions {
		for _, p := range ps {
			delete(t.parts, topicPartition{topic, p})
		}
	}
}
