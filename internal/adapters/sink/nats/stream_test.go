// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package nats_test

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	natssink "codeberg.org/eljoth/tram/internal/adapters/sink/nats"
)

func TestSubjectCovers(t *testing.T) {
	t.Parallel()

	cases := []struct {
		pattern, subject string
		expected         bool
	}{
		{pattern: "logs.a", subject: "logs.a", expected: true},
		{pattern: "logs.a", subject: "logs.b", expected: false},
		{pattern: "logs.*", subject: "logs.a", expected: true},
		{pattern: "logs.*", subject: "logs.*", expected: true},
		{pattern: "logs.*", subject: "logs.a.b", expected: false},
		{pattern: "logs.*", subject: "logs.>", expected: false},
		{pattern: "logs.>", subject: "logs.a", expected: true},
		{pattern: "logs.>", subject: "logs.a.b", expected: true},
		{pattern: "logs.>", subject: "logs.*", expected: true},
		{pattern: "logs.>", subject: "logs.>", expected: true},
		{pattern: "logs.>", subject: "logs", expected: false},
		{pattern: ">", subject: "anything.at.all", expected: true},
		{pattern: "logs.a", subject: "logs.*", expected: false},
		{pattern: "logs.a", subject: "logs.a.b", expected: false},
		{pattern: "logs.a.b", subject: "logs.a", expected: false},
		{pattern: "*.a", subject: "logs.a", expected: true},
	}

	for _, c := range cases {
		assert.Equal(t, c.expected, natssink.SubjectCovers(c.pattern, c.subject), "%q covers %q", c.pattern, c.subject)
	}
}

func TestMergeStreamConfig(t *testing.T) {
	t.Parallel()

	existing := func() jetstream.StreamConfig {
		return jetstream.StreamConfig{
			Name:     "telemetry",
			Subjects: []string{"logs.>"},
			MaxAge:   time.Hour,
			MaxBytes: 1000,
			Replicas: 3,
			Storage:  jetstream.MemoryStorage,
			Discard:  jetstream.DiscardNew,
		}
	}
	spec := func(register ...string) *natssink.Spec {
		return &natssink.Spec{Stream: "telemetry", Subject: natssink.SubjectSpec{Register: register}}
	}

	t.Run("covered subjects and no limits change nothing", func(t *testing.T) {
		t.Parallel()

		merged, update, err := natssink.MergeStreamConfig(existing(), spec("logs.>", "logs.a", "logs.*.b"))
		require.NoError(t, err)
		assert.False(t, update.Changed)
		assert.Equal(t, existing(), merged, "unset limits and other settings are kept")
	})

	t.Run("adds missing subjects and keeps the others", func(t *testing.T) {
		t.Parallel()

		in := existing()
		merged, update, err := natssink.MergeStreamConfig(in, spec("metrics.>", "metrics.>", "traces.a"))
		require.NoError(t, err)
		assert.True(t, update.Changed)
		assert.Equal(t, []string{"metrics.>", "traces.a"}, update.Subjects)

		expected := existing()
		expected.Subjects = []string{"logs.>", "metrics.>", "traces.a"}
		assert.Equal(t, expected, merged)
		assert.Equal(t, []string{"logs.>"}, in.Subjects, "the existing config is not modified")
	})

	t.Run("applies limits set in the spec", func(t *testing.T) {
		t.Parallel()

		s := spec("logs.>")
		s.MaxAge = "2h"
		s.Size = 2000
		merged, update, err := natssink.MergeStreamConfig(existing(), s)
		require.NoError(t, err)
		assert.True(t, update.Changed)
		assert.Equal(t, 2*time.Hour, merged.MaxAge)
		assert.Equal(t, int64(2000), merged.MaxBytes)
		assert.Equal(t, []string{"max_age 1h0m0s -> 2h0m0s", "size 1000 -> 2000"}, update.ReplacedLimits)
		assert.Equal(t, 3, merged.Replicas, "other settings are kept")
	})

	t.Run("setting a limit the stream does not have replaces nothing", func(t *testing.T) {
		t.Parallel()

		in := existing()
		in.MaxAge = 0
		in.MaxBytes = -1
		s := spec("logs.>")
		s.MaxAge = "2h"
		s.Size = 2000
		_, update, err := natssink.MergeStreamConfig(in, s)
		require.NoError(t, err)
		assert.True(t, update.Changed)
		assert.Empty(t, update.ReplacedLimits)
	})

	t.Run("applies compression set in the spec", func(t *testing.T) {
		t.Parallel()

		s := spec("logs.>")
		s.Compression = "s2"
		merged, update, err := natssink.MergeStreamConfig(existing(), s)
		require.NoError(t, err)
		assert.True(t, update.Changed)
		assert.Equal(t, jetstream.S2Compression, merged.Compression)
		assert.Empty(t, update.ReplacedLimits, "turning compression on replaces no setting")
	})

	t.Run("replacing a compression is reported", func(t *testing.T) {
		t.Parallel()

		in := existing()
		in.Compression = jetstream.S2Compression
		s := spec("logs.>")
		s.Compression = "none"
		merged, update, err := natssink.MergeStreamConfig(in, s)
		require.NoError(t, err)
		assert.True(t, update.Changed)
		assert.Equal(t, jetstream.NoCompression, merged.Compression)
		assert.Equal(t, []string{"compression s2 -> none"}, update.ReplacedLimits)
	})

	t.Run("a spec without compression keeps the stream's", func(t *testing.T) {
		t.Parallel()

		in := existing()
		in.Compression = jetstream.S2Compression
		merged, update, err := natssink.MergeStreamConfig(in, spec("logs.>"))
		require.NoError(t, err)
		assert.False(t, update.Changed)
		assert.Equal(t, jetstream.S2Compression, merged.Compression)
	})

	t.Run("invalid retention", func(t *testing.T) {
		t.Parallel()

		s := spec("logs.>")
		s.Retention = "invalid"
		_, _, err := natssink.MergeStreamConfig(existing(), s)
		assert.ErrorContains(t, err, "invalid retention policy")
	})

	t.Run("invalid max_age", func(t *testing.T) {
		t.Parallel()

		s := spec("logs.>")
		s.MaxAge = "invalid"
		_, _, err := natssink.MergeStreamConfig(existing(), s)
		assert.ErrorContains(t, err, "invalid max_age")
	})

	t.Run("applies retention policy set in the spec", func(t *testing.T) {
		t.Parallel()

		s := spec("logs.>")
		s.Retention = "interest"
		merged, update, err := natssink.MergeStreamConfig(existing(), s)
		require.NoError(t, err)
		assert.True(t, update.Changed)
		assert.Equal(t, jetstream.InterestPolicy, merged.Retention)
		assert.Equal(
			t,
			[]string(nil),
			update.ReplacedLimits,
			"setting retention policy from default does not replace an existing limit",
		)
		assert.Equal(t, 1*time.Hour, merged.MaxAge, "MaxAge is kept from existing config")
	})

	t.Run("applies max_age set in the spec", func(t *testing.T) {
		t.Parallel()

		s := spec("logs.>")
		s.MaxAge = "3h"
		merged, update, err := natssink.MergeStreamConfig(existing(), s)
		require.NoError(t, err)
		assert.True(t, update.Changed)
		assert.Equal(t, 3*time.Hour, merged.MaxAge)
		assert.Equal(t, []string{"max_age 1h0m0s -> 3h0m0s"}, update.ReplacedLimits)
	})

	t.Run("applies both retention policy and max_age", func(t *testing.T) {
		t.Parallel()

		s := spec("logs.>")
		s.Retention = "workqueue"
		s.MaxAge = "4h"
		merged, update, err := natssink.MergeStreamConfig(existing(), s)
		require.NoError(t, err)
		assert.True(t, update.Changed)
		assert.Equal(t, jetstream.WorkQueuePolicy, merged.Retention)
		assert.Equal(t, 4*time.Hour, merged.MaxAge)
		assert.Equal(t, []string{"max_age 1h0m0s -> 4h0m0s"}, update.ReplacedLimits)
	})

	t.Run("replacing retention policy is reported", func(t *testing.T) {
		t.Parallel()

		in := existing()
		in.Retention = jetstream.InterestPolicy
		s := spec("logs.>")
		s.Retention = "workqueue"
		merged, update, err := natssink.MergeStreamConfig(in, s)
		require.NoError(t, err)
		assert.True(t, update.Changed)
		assert.Equal(t, jetstream.WorkQueuePolicy, merged.Retention)
		assert.Equal(t, []string{"retention interest -> workqueue"}, update.ReplacedLimits)
	})

	// LimitsPolicy is the zero value of the policy, so it must still count as
	// set when the spec names it.
	t.Run("limits retention replaces another policy", func(t *testing.T) {
		t.Parallel()

		in := existing()
		in.Retention = jetstream.InterestPolicy
		s := spec("logs.>")
		s.Retention = "limits"
		merged, update, err := natssink.MergeStreamConfig(in, s)
		require.NoError(t, err)
		assert.True(t, update.Changed)
		assert.Equal(t, jetstream.LimitsPolicy, merged.Retention)
		assert.Equal(t, []string{"retention interest -> limits"}, update.ReplacedLimits)
	})

	t.Run("a spec without retention keeps the stream's", func(t *testing.T) {
		t.Parallel()

		in := existing()
		in.Retention = jetstream.InterestPolicy
		merged, update, err := natssink.MergeStreamConfig(in, spec("logs.>"))
		require.NoError(t, err)
		assert.False(t, update.Changed)
		assert.Equal(t, jetstream.InterestPolicy, merged.Retention)
	})

	t.Run("replacing replicas is reported", func(t *testing.T) {
		t.Parallel()

		s := spec("logs.>")
		s.Replicas = 1
		merged, update, err := natssink.MergeStreamConfig(existing(), s)
		require.NoError(t, err)
		assert.True(t, update.Changed)
		assert.Equal(t, 1, merged.Replicas)
		assert.Equal(t, []string{"replicas 3 -> 1"}, update.ReplacedLimits)
	})

	t.Run("same replicas change nothing", func(t *testing.T) {
		t.Parallel()

		s := spec("logs.>")
		s.Replicas = 3
		_, update, err := natssink.MergeStreamConfig(existing(), s)
		require.NoError(t, err)
		assert.False(t, update.Changed)
	})

	// DiscardOld is the zero value of the policy, so it must still count as
	// set when the spec names it.
	t.Run("old discard replaces new", func(t *testing.T) {
		t.Parallel()

		s := spec("logs.>")
		s.Discard = "old"
		merged, update, err := natssink.MergeStreamConfig(existing(), s)
		require.NoError(t, err)
		assert.True(t, update.Changed)
		assert.Equal(t, jetstream.DiscardOld, merged.Discard)
		assert.Equal(t, []string{"discard new -> old"}, update.ReplacedLimits)
	})

	t.Run("setting discard from the default replaces nothing", func(t *testing.T) {
		t.Parallel()

		in := existing()
		in.Discard = jetstream.DiscardOld
		s := spec("logs.>")
		s.Discard = "new"
		merged, update, err := natssink.MergeStreamConfig(in, s)
		require.NoError(t, err)
		assert.True(t, update.Changed)
		assert.Equal(t, jetstream.DiscardNew, merged.Discard)
		assert.Empty(t, update.ReplacedLimits)
	})

	t.Run("matching storage changes nothing", func(t *testing.T) {
		t.Parallel()

		s := spec("logs.>")
		s.Storage = "memory"
		_, update, err := natssink.MergeStreamConfig(existing(), s)
		require.NoError(t, err)
		assert.False(t, update.Changed)
	})

	// FileStorage is the zero value, so requiring it of a memory stream must
	// still fail.
	t.Run("different storage fails", func(t *testing.T) {
		t.Parallel()

		s := spec("logs.>")
		s.Storage = "file"
		_, _, err := natssink.MergeStreamConfig(existing(), s)
		require.ErrorIs(t, err, natssink.ErrStorageMismatch)
		assert.ErrorContains(t, err, "stream telemetry uses memory storage, the sink requires file")
	})
}
