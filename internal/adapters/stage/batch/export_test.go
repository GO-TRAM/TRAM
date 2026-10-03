package batch

import (
	"time"

	"codeberg.org/eljoth/tram/internal/core/domain"
)

const (
	DefaultMaxRecords = defaultMaxRecords
	DefaultTimeout    = defaultTimeout
)

// OpenRecords returns the records in the open batch and whether one is open.
func OpenRecords(b *Batch) (int, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.current == nil {
		return 0, false
	}
	return b.current.records, true
}

// Limits returns the configured flush thresholds.
func Limits(b *Batch) (int, time.Duration) {
	return b.maxRecords, b.timeout
}

// Merge merges events as a flush does.
func Merge(events ...*domain.Event) *domain.Event {
	entries := make([]*entry, len(events))
	for i, e := range events {
		entries[i] = &entry{event: e}
	}
	return merge(entries)
}
