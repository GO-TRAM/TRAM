// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package e2e_test

import (
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"

	"codeberg.org/eljoth/tram/internal/core/domain"
)

func batchStage(maxRecords int, timeout string) domain.StageSpec {
	return domain.StageSpec{Name: "batch", Type: "batch", From: "in", Spec: map[string]any{
		"max_records": float64(maxRecords),
		"timeout":     timeout,
	}}
}

func TestBatchMergesConcurrentRequests(t *testing.T) {
	t.Parallel()

	p := newPipeline(t, []domain.StageSpec{batchStage(6, "5s")}, []domain.SinkSpec{captureSinkSpec("out", "batch")})

	codes := p.postConcurrently(t,
		request("checkout", rec{"c1", "A"}, rec{"c2", "B"}),
		request("cart", rec{"k1", "A"}, rec{"k2", "B"}),
		request("search", rec{"s1", "A"}, rec{"s2", "B"}),
	)

	assert.Equal(
		t,
		[]int{http.StatusOK, http.StatusOK, http.StatusOK},
		codes,
		"every caller gets the result of the delivered batch",
	)
	events := p.sinks.sink(t, "out").received()
	require.Len(t, events, 1, "the three requests are delivered as one batch")
	assert.Len(t, events[0].Logs.GetResourceLogs(), 3, "each request keeps its resource entry")
	assert.ElementsMatch(t, []string{
		"checkout/e2e/c1", "checkout/e2e/c2",
		"cart/e2e/k1", "cart/e2e/k2",
		"search/e2e/s1", "search/e2e/s2",
	}, arrived(events))
}

func TestBatchFlushesAfterTimeout(t *testing.T) {
	t.Parallel()

	p := newPipeline(t, []domain.StageSpec{batchStage(100, "50ms")}, []domain.SinkSpec{captureSinkSpec("out", "batch")})

	start := time.Now()
	assert.Equal(t, http.StatusOK, p.post(t, request("checkout", rec{"c1", "A"}), 10*time.Second))
	assert.GreaterOrEqual(t, time.Since(start), 50*time.Millisecond, "the request waits for the batch timeout")
	assert.Equal(t, []string{"checkout/e2e/c1"}, arrived(p.sinks.sink(t, "out").received()))
}

func TestCancelledRequestIsNotDelivered(t *testing.T) {
	t.Parallel()

	p := newPipeline(
		t,
		[]domain.StageSpec{batchStage(100, "500ms")},
		[]domain.SinkSpec{captureSinkSpec("out", "batch")},
	)

	var wg sync.WaitGroup
	var cancelled, kept int
	wg.Go(func() { cancelled = p.post(t, request("checkout", rec{"gone", "A"}), 100*time.Millisecond) })
	wg.Go(func() {
		time.Sleep(20 * time.Millisecond)
		kept = p.post(t, request("cart", rec{"stays", "A"}), 10*time.Second)
	})
	wg.Wait()

	assert.Equal(t, 0, cancelled, "the first client timed out")
	assert.Equal(t, http.StatusOK, kept)
	assert.Equal(t, []string{"cart/e2e/stays"}, arrived(p.sinks.sink(t, "out").received()))
}

func TestSplitByCustomer(t *testing.T) {
	t.Parallel()

	p := newPipeline(t,
		[]domain.StageSpec{
			customerFilter("customer-a", "in", isCustomerA),
			customerFilter("other-customers", "in", "!"+isCustomerA),
		},
		[]domain.SinkSpec{
			captureSinkSpec("sink-a", "customer-a"),
			captureSinkSpec("sink-rest", "other-customers"),
		})

	assert.Equal(
		t,
		http.StatusOK,
		p.post(t, request("shop", rec{"r1", "A"}, rec{"r2", "B"}, rec{"r3", "A"}, rec{"r4", "C"}), 10*time.Second),
	)

	assert.Equal(
		t,
		[]string{"shop/e2e/r1", "shop/e2e/r3"},
		arrived(p.sinks.sink(t, "sink-a").received()),
		"order within the branch is kept",
	)
	assert.Equal(t, []string{"shop/e2e/r2", "shop/e2e/r4"}, arrived(p.sinks.sink(t, "sink-rest").received()))
}

func TestSplitWithoutMatchesDeliversNothing(t *testing.T) {
	t.Parallel()

	p := newPipeline(t,
		[]domain.StageSpec{customerFilter("customer-a", "in", isCustomerA)},
		[]domain.SinkSpec{captureSinkSpec("sink-a", "customer-a")})

	assert.Equal(t, http.StatusOK, p.post(t, request("shop", rec{"r1", "B"}), 10*time.Second))
	assert.Empty(t, p.sinks.sink(t, "sink-a").received(), "a branch without matching records receives no event")
}

func TestBatchThenSplit(t *testing.T) {
	t.Parallel()

	p := newPipeline(t,
		[]domain.StageSpec{
			batchStage(4, "5s"),
			customerFilter("customer-a", "batch", isCustomerA),
			customerFilter("other-customers", "batch", "!"+isCustomerA),
		},
		[]domain.SinkSpec{
			captureSinkSpec("sink-a", "customer-a"),
			captureSinkSpec("sink-rest", "other-customers"),
		})

	codes := p.postConcurrently(t,
		request("checkout", rec{"c1", "A"}, rec{"c2", "B"}),
		request("cart", rec{"k1", "C"}, rec{"k2", "A"}),
	)
	assert.Equal(t, []int{http.StatusOK, http.StatusOK}, codes)

	toA := p.sinks.sink(t, "sink-a").received()
	toRest := p.sinks.sink(t, "sink-rest").received()
	require.Len(t, toA, 1, "each branch receives the batch as one event")
	require.Len(t, toRest, 1)
	assert.ElementsMatch(t, []string{"checkout/e2e/c1", "cart/e2e/k2"}, arrived(toA))
	assert.ElementsMatch(t, []string{"checkout/e2e/c2", "cart/e2e/k1"}, arrived(toRest))
}

func TestDeliveryResultReachesClient(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		stages   []domain.StageSpec
		sink     domain.SinkSpec
		payload  *logspb.LogsData
		expected int
		arrives  bool
	}{
		{
			name:     "delivered",
			sink:     captureSinkSpec("out", "in"),
			payload:  request("shop", rec{"r1", "A"}),
			expected: http.StatusOK,
			arrives:  true,
		},
		{
			name: "rejected by cel_validator",
			stages: []domain.StageSpec{{Name: "validate", Type: "cel_validator", From: "in", Spec: map[string]any{
				"logs":        "logs.resource_logs.all(r, r.resource.attributes.exists(a, a.key == 'service.name' && a.value.string_value == 'known'))",
				"on_mismatch": "reject",
			}}},
			sink:     captureSinkSpec("out", "validate"),
			payload:  request("unknown", rec{"r1", "A"}),
			expected: http.StatusUnprocessableEntity,
		},
		{
			name: "permanent sink error",
			sink: domain.SinkSpec{
				Name: "out",
				Type: captureType,
				From: "in",
				Spec: map[string]any{"fail": "permanent"},
			},
			payload:  request("shop", rec{"r1", "A"}),
			expected: http.StatusUnprocessableEntity,
		},
		{
			name: "temporary sink error",
			sink: domain.SinkSpec{
				Name: "out",
				Type: captureType,
				From: "in",
				Spec: map[string]any{"fail": "temporary"},
			},
			payload:  request("shop", rec{"r1", "A"}),
			expected: http.StatusServiceUnavailable,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			p := newPipeline(t, c.stages, []domain.SinkSpec{c.sink})
			assert.Equal(t, c.expected, p.post(t, c.payload, 10*time.Second))
			if c.arrives {
				assert.NotEmpty(t, p.sinks.sink(t, "out").received())
			} else {
				assert.Empty(t, p.sinks.sink(t, "out").received())
			}
		})
	}
}

func TestUpdateUnderLoadKeepsEveryRecord(t *testing.T) {
	t.Parallel()

	p := newPipeline(t, nil, []domain.SinkSpec{captureSinkSpec("before", "in")})

	const clients, perClient = 4, 50
	var sent atomic.Int64
	var failed atomic.Int64
	var wg sync.WaitGroup
	for c := range clients {
		wg.Go(func() {
			for i := range perClient {
				r := request("shop", rec{id: idOf(c, i), customer: "A"})
				if p.post(t, r, 10*time.Second) != http.StatusOK {
					failed.Add(1)
				}
				sent.Add(1)
			}
		})
	}

	require.Eventually(t, func() bool { return sent.Load() >= clients*perClient/2 }, 10*time.Second, time.Millisecond)
	p.update(
		t,
		[]domain.StageSpec{customerFilter("filter", "in", isCustomerA)},
		[]domain.SinkSpec{captureSinkSpec("after", "filter")},
	)
	wg.Wait()

	assert.Zero(t, failed.Load(), "no request failed during the update")
	got := append(arrived(p.sinks.sink(t, "before").received()), arrived(p.sinks.sink(t, "after").received())...)
	var want []string
	for c := range clients {
		for i := range perClient {
			want = append(want, "shop/e2e/"+idOf(c, i))
		}
	}
	assert.ElementsMatch(t, want, got, "every record arrived exactly once")
	assert.NotEmpty(t, p.sinks.sink(t, "after").received(), "records after the update go to the new sink")
}

func idOf(client, i int) string {
	return fmt.Sprintf("client%d-%02d", client, i)
}
