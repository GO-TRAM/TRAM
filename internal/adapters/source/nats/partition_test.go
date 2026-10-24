// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package nats_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"codeberg.org/eljoth/tram/internal/adapters/natspartition"
	natssource "codeberg.org/eljoth/tram/internal/adapters/source/nats"
)

func sourceSpec(stream, subject string, partition map[string]any) map[string]any {
	spec := map[string]any{"url": "nats://localhost:4222", "stream": stream, "subject": subject}
	if partition != nil {
		spec["partition"] = partition
	}
	return spec
}

func TestTargetOfPartition(t *testing.T) {
	t.Parallel()

	for _, key := range []string{"tenant-1", "tenant-2", "tenant-3"} {
		p := natspartition.Of(key, 8)
		target, err := natssource.TargetOf(
			sourceSpec("audit-{partition}", "audit.{partition}."+key, map[string]any{"count": 8, "key": key}))
		require.NoError(t, err)
		assert.Equal(t, natssource.Target{
			Stream:  fmt.Sprintf("audit-%d", p),
			Subject: fmt.Sprintf("audit.%d.%s", p, key),
		}, target, "key %q", key)
	}
}

func TestTargetOfWithoutPartition(t *testing.T) {
	t.Parallel()

	target, err := natssource.TargetOf(sourceSpec("audit", "audit.>", nil))
	require.NoError(t, err)
	assert.Equal(t, natssource.Target{Stream: "audit", Subject: "audit.>"}, target)
}

func TestTargetOfRejectsInvalidPartition(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		spec map[string]any
		err  string
	}{
		{
			name: "stream without placeholder",
			spec: sourceSpec("audit", "audit.>", map[string]any{"count": 8, "key": "t"}),
			err:  "stream audit needs {partition}",
		},
		{
			name: "placeholder in stream without partition",
			spec: sourceSpec("audit-{partition}", "audit.>", nil),
			err:  "stream audit-{partition} has {partition}, but partition is not set",
		},
		{
			name: "placeholder in subject without partition",
			spec: sourceSpec("audit", "audit.{partition}.>", nil),
			err:  "subject audit.{partition}.> has {partition}, but partition is not set",
		},
		{name: "missing key", spec: sourceSpec("audit-{partition}", "audit.>", map[string]any{"count": 8})},
		{name: "count 0", spec: sourceSpec("audit-{partition}", "audit.>", map[string]any{"count": 0, "key": "t"})},
		{name: "count above 1024", spec: sourceSpec("audit-{partition}", "audit.>", map[string]any{"count": 1025})},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			_, err := natssource.TargetOf(c.spec)
			require.Error(t, err)
			if c.err != "" {
				require.ErrorIs(t, err, natssource.ErrInvalidPartition)
				assert.ErrorContains(t, err, c.err)
			}
		})
	}
}
