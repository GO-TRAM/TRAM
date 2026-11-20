// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package engines_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/runtime"

	tramv1 "github.com/go-tram/tram/internal/adapters/k8scrd/api/v1"
	"github.com/go-tram/tram/internal/adapters/k8scrd/internal/engines"
)

func TestClaimOrdinal(t *testing.T) {
	t.Parallel()

	cases := []struct {
		claim   string
		ordinal int
		ok      bool
	}{
		{claim: "data-tram-engine-edge-0", ordinal: 0, ok: true},
		{claim: "data-tram-engine-edge-12", ordinal: 12, ok: true},
		{claim: "data-tram-engine-edge-x", ok: false},
		{claim: "data-tram-engine-edge-01", ok: false},
		{claim: "data-tram-engine-edge-v2-0", ok: false},
		{claim: "other-0", ok: false},
	}
	for _, tc := range cases {
		ordinal, ok := engines.ClaimOrdinal("edge", tc.claim)
		assert.Equal(t, tc.ok, ok, tc.claim)
		assert.Equal(t, tc.ordinal, ordinal, tc.claim)
	}
	assert.Equal(t, "data-tram-engine-edge-3", engines.ClaimName("edge", 3))
}

func bufferStream(specs ...string) tramv1.Stream {
	var s tramv1.Stream
	for _, spec := range specs {
		s.Spec.Sinks = append(s.Spec.Sinks, tramv1.ReceiverSpec{
			Name: "b", Type: "buffer", Spec: &runtime.RawExtension{Raw: []byte(spec)},
		})
	}
	return s
}

func TestMaxBufferAge(t *testing.T) {
	t.Parallel()

	assert.Equal(t, 24*time.Hour, engines.MaxBufferAge(nil), "the default without buffers")
	assert.Equal(t, 24*time.Hour, engines.MaxBufferAge([]tramv1.Stream{bufferStream(`{"max_age":"1h"}`)}),
		"never below the default: a volume may hold files of streams that left")
	assert.Equal(t, 72*time.Hour, engines.MaxBufferAge([]tramv1.Stream{
		bufferStream(`{"max_age":"1h"}`, `{"max_age":"72h"}`),
		bufferStream(`{"max_age":"invalid"}`),
	}))
}
