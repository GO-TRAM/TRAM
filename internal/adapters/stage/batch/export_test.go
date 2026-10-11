// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package batch

import (
	"time"

	"codeberg.org/eljoth/tram/internal/core/domain"
)

const (
	DefaultMaxRecords = defaultMaxRecords
	DefaultTimeout    = defaultTimeout
)

func OpenRecords(b *Batch) (int, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.current == nil {
		return 0, false
	}
	return b.current.records, true
}

func Limits(b *Batch) (int, time.Duration) {
	return b.maxRecords, b.timeout
}

func Merge(events ...*domain.Event) *domain.Event {
	entries := make([]*entry, len(events))
	for i, e := range events {
		entries[i] = &entry{event: e}
	}
	return merge(entries)
}
