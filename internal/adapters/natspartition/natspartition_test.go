// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package natspartition_test

import (
	"hash/fnv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"codeberg.org/eljoth/tram/internal/adapters/natspartition"
)

func TestOfIsFNV1a(t *testing.T) {
	t.Parallel()

	for _, key := range []string{"", "a", "tenant-123", "ümlaut", "a much longer key with spaces"} {
		h := fnv.New32a()
		_, _ = h.Write([]byte(key))
		for _, count := range []int{1, 7, 8, 1024} {
			assert.Equal(t, int(h.Sum32()%uint32(count)), natspartition.Of(key, count), "%q into %d", key, count)
		}
	}
}

// Consumers outside Tram rely on these values, so they must never change.
func TestOfIsStable(t *testing.T) {
	t.Parallel()

	assert.Equal(t, 5, natspartition.Of("", 8))
	assert.Equal(t, 4, natspartition.Of("a", 8))
	assert.Equal(t, 0, natspartition.Of("anything", 1))
}

func TestExpand(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "audit.3.>", natspartition.Expand("audit.{partition}.>", 3))
	assert.Equal(t, "p12-p12", natspartition.Expand("p{partition}-p{partition}", 12))
	assert.Equal(t, "audit.>", natspartition.Expand("audit.>", 3))
	assert.True(t, natspartition.HasPlaceholder("audit-{partition}"))
	assert.False(t, natspartition.HasPlaceholder("audit"))
}

func TestCheck(t *testing.T) {
	t.Parallel()

	require.NoError(t, natspartition.Check("s", natspartition.Metadata(2, 8), 2, 8))
	require.NoError(t, natspartition.Check("s", nil, 2, 8), "a stream without partition metadata passes")
	require.NoError(t, natspartition.Check("s", map[string]string{"other": "x"}, 2, 8))

	err := natspartition.Check("audit-2", natspartition.Metadata(2, 4), 2, 8)
	require.ErrorIs(t, err, natspartition.ErrMismatch)
	require.ErrorContains(t, err, "stream audit-2 has tram.partition.count 4, expected 8")

	err = natspartition.Check("audit-2", natspartition.Metadata(3, 8), 2, 8)
	require.ErrorIs(t, err, natspartition.ErrMismatch)
}
