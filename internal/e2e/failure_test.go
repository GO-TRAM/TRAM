// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package e2e_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/go-tram/tram/internal/core/domain"
)

// A failing branch does not keep the event from the other branches, but the
// client learns about the failure and sends the request again. Delivery is
// at least once: the healthy branch gets the retried records a second time.
func TestFailingBranchDoesNotStopTheOtherBranches(t *testing.T) {
	t.Parallel()

	p := newPipeline(t, nil, []domain.SinkSpec{
		captureSinkSpec("healthy", "in"),
		{Name: "down", Type: captureType, From: "in", Spec: map[string]any{"fail": "temporary"}},
	})

	payload := request("shop", rec{"r1", "A"})
	assert.Equal(t, http.StatusServiceUnavailable, p.post(t, payload, 10*time.Second),
		"the client is told to retry")
	assert.Equal(t, []string{"shop/e2e/r1"}, arrived(p.sinks.sink(t, "healthy").received()),
		"the healthy branch got the record anyway")

	assert.Equal(t, http.StatusServiceUnavailable, p.post(t, payload, 10*time.Second))
	assert.Equal(t, []string{"shop/e2e/r1", "shop/e2e/r1"}, arrived(p.sinks.sink(t, "healthy").received()),
		"the retry reaches the healthy branch again")
	assert.Empty(t, p.sinks.sink(t, "down").received())
}

// A sink that fails for a while neither stops nor restarts the stream: once
// it works again, the retried request goes through.
func TestStreamRecoversFromTransientSinkFailures(t *testing.T) {
	t.Parallel()

	const failures = 3
	p := newPipeline(t,
		[]domain.StageSpec{customerFilter(customerA, "in", isCustomerA)},
		[]domain.SinkSpec{{
			Name: "flaky", Type: captureType, From: customerA,
			Spec: map[string]any{"fail_first": float64(failures)},
		}})

	payload := request("shop", rec{"r1", "A"})
	for range failures {
		require.Equal(t, http.StatusServiceUnavailable, p.post(t, payload, 10*time.Second))
	}
	require.Equal(t, http.StatusOK, p.post(t, payload, 10*time.Second), "the stream works again")
	require.Equal(t, http.StatusOK, p.post(t, request("shop", rec{"r2", "A"}), 10*time.Second))

	assert.Equal(t, []string{"shop/e2e/r1", "shop/e2e/r2"}, arrived(p.sinks.sink(t, "flaky").received()),
		"each record arrives once it is accepted")
}

// Records rejected for good by one branch are reported to the client; the
// other branch still gets them, and the client must not retry them.
func TestRejectingBranchDoesNotStopTheOtherBranches(t *testing.T) {
	t.Parallel()

	p := newPipeline(t, nil, []domain.SinkSpec{
		captureSinkSpec("healthy", "in"),
		{Name: "refusing", Type: captureType, From: "in", Spec: map[string]any{"fail": "permanent"}},
	})

	assert.Equal(t, http.StatusUnprocessableEntity, p.post(t, request("shop", rec{"r1", "A"}), 10*time.Second))
	assert.Equal(t, []string{"shop/e2e/r1"}, arrived(p.sinks.sink(t, "healthy").received()))
}
