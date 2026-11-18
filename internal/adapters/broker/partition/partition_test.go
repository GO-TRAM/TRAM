// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package partition_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"codeberg.org/eljoth/tram/internal/adapters/broker/partition"
	"codeberg.org/eljoth/tram/internal/adapters/natspartition"
)

func TestOfIsStable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		key   string
		count int
		want  int
	}{
		// Fixed values: consumers compute the partition themselves, so the
		// hash must never change.
		{key: "", count: 8, want: 5},
		{key: "tenant-123", count: 8, want: partition.Of("tenant-123", 8)},
		{key: "a", count: 1, want: 0},
	}
	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, partition.Of(tt.key, tt.count))
		})
	}
}

// The broker sink must spread keys exactly like the NATS sink, so data
// written by one is found by consumers of the other.
func TestOfMatchesNATSPartition(t *testing.T) {
	t.Parallel()

	for _, key := range []string{"", "a", "tenant-123", "f9b77c12-7d75-039f-e93b-3f7224af834a"} {
		for _, count := range []int{1, 3, 8, 1024} {
			assert.Equal(t, natspartition.Of(key, count), partition.Of(key, count), "key %q count %d", key, count)
		}
	}
}

func TestExpandAll(t *testing.T) {
	t.Parallel()

	in := map[string]any{
		"url":     "nats://n:4222",
		"subject": "audit.{partition}.>",
		"list":    []any{"s-{partition}", 3},
		"nested":  map[string]any{"stream": "audit-{partition}"},
	}
	out, found := partition.ExpandAll(in, 7)
	assert.True(t, found)
	assert.Equal(t, map[string]any{
		"url":     "nats://n:4222",
		"subject": "audit.7.>",
		"list":    []any{"s-7", 3},
		"nested":  map[string]any{"stream": "audit-7"},
	}, out)
	assert.Equal(t, "audit.{partition}.>", in["subject"], "the input is not modified")

	_, found = partition.ExpandAll(map[string]any{"url": "x"}, 1)
	assert.False(t, found)
}
