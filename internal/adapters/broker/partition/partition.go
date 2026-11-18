// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

// Package partition spreads keys over a fixed number of partitions. The
// broker sink and source share it, so a consumer finds the partition its key
// was published to.
package partition

import (
	"strconv"
	"strings"
)

const (
	// Placeholder is replaced by the partition number, e.g. in subjects.
	Placeholder = "{partition}"

	MaxCount = 1024
)

// FNV-1a, 32 bit. Changing it moves keys to other partitions, so it must stay.
const (
	fnvOffset = 2166136261
	fnvPrime  = 16777619
)

// Of returns the partition of key, from 0 to count-1.
func Of(key string, count int) int {
	h := uint32(fnvOffset)
	for i := range len(key) {
		h ^= uint32(key[i])
		h *= fnvPrime
	}
	return int(h % uint32(count)) //nolint:gosec // count is between 1 and MaxCount.
}

// Expand replaces the placeholder in s with partition.
func Expand(s string, partition int) string {
	return strings.ReplaceAll(s, Placeholder, strconv.Itoa(partition))
}

// HasPlaceholder reports whether s contains the placeholder.
func HasPlaceholder(s string) bool {
	return strings.Contains(s, Placeholder)
}

// ExpandAll returns a copy of value in which every string has the placeholder
// replaced, and whether there was any. It is used on driver options, whose
// fields only the driver knows.
func ExpandAll(value any, partition int) (any, bool) {
	switch v := value.(type) {
	case string:
		return Expand(v, partition), HasPlaceholder(v)
	case map[string]any:
		out := make(map[string]any, len(v))
		found := false
		for key, item := range v {
			expanded, itemFound := ExpandAll(item, partition)
			out[key] = expanded
			found = found || itemFound
		}
		return out, found
	case []any:
		out := make([]any, len(v))
		found := false
		for i, item := range v {
			expanded, itemFound := ExpandAll(item, partition)
			out[i] = expanded
			found = found || itemFound
		}
		return out, found
	default:
		return value, false
	}
}
