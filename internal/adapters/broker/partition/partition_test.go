// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package partition_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"codeberg.org/eljoth/tram/internal/adapters/broker/partition"
)

// Fixed values, computed with the partitioning of the former NATS sink: data
// partitioned before is still found, and consumers that compute the
// partition themselves keep working. The hash must never change.
func TestOfIsStable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		key   string
		count int
		want  int
	}{
		{key: "", count: 1, want: 0},
		{key: "", count: 3, want: 1},
		{key: "", count: 8, want: 5},
		{key: "", count: 1024, want: 453},
		{key: "a", count: 3, want: 1},
		{key: "a", count: 8, want: 4},
		{key: "a", count: 1024, want: 300},
		{key: "tenant-123", count: 3, want: 0},
		{key: "tenant-123", count: 8, want: 0},
		{key: "tenant-123", count: 1024, want: 152},
		{key: "f9b77c12-7d75-039f-e93b-3f7224af834a", count: 3, want: 2},
		{key: "f9b77c12-7d75-039f-e93b-3f7224af834a", count: 8, want: 3},
		{key: "f9b77c12-7d75-039f-e93b-3f7224af834a", count: 1024, want: 499},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%s/%d", tt.key, tt.count), func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, partition.Of(tt.key, tt.count))
		})
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
