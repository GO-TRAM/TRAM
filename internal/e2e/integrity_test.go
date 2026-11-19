// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package e2e_test

import (
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"codeberg.org/eljoth/tram/internal/core/domain"
)

// Many clients send at once; a batch merges their requests, and the stream
// splits the batches by customer while a third branch gets everything. Every
// record must reach each branch it belongs to exactly once and no other.
func TestConcurrentLoadKeepsEveryRecordInItsBranches(t *testing.T) {
	t.Parallel()

	p := newPipeline(t,
		[]domain.StageSpec{
			batchStage(30, "20ms"),
			customerFilter(customerA, "batch", isCustomerA),
			customerFilter(otherCustomers, "batch", "!"+isCustomerA),
		},
		[]domain.SinkSpec{
			captureSinkSpec(sinkA, customerA),
			captureSinkSpec(sinkRest, otherCustomers),
			captureSinkSpec("all", "in"),
		})

	const clients, perClient = 8, 25
	customers := []string{"A", "B", "C"}
	require.Empty(t, sendFromClients(t, p, clients, perClient, customers), "every request is answered with 200")
	want, wantA, wantRest := expectedRecords(clients, perClient, customers)

	all := arrived(p.sinks.sink(t, "all").received())
	toA := arrived(p.sinks.sink(t, sinkA).received())
	toRest := arrived(p.sinks.sink(t, sinkRest).received())
	assert.ElementsMatch(t, want, all, "the unfiltered branch gets every record exactly once")
	assert.ElementsMatch(t, wantA, toA, "customer A gets exactly its records")
	assert.ElementsMatch(t, wantRest, toRest, "the other customers get exactly theirs")
	for _, id := range toRest {
		assert.False(t, strings.HasSuffix(id, "-A"), "record %s of customer A leaked into the other branch", id)
	}
	assert.Less(t, len(p.sinks.sink(t, sinkA).received()), clients*perClient,
		"the batch merged requests, so the split worked on mixed batches")
}

// sendFromClients sends perClient requests from each client at the same time,
// each with one record per customer, and returns the status codes other
// than 200.
func sendFromClients(t *testing.T, p *pipeline, clients, perClient int, customers []string) []int {
	t.Helper()
	var mu sync.Mutex
	var failed []int
	var wg sync.WaitGroup
	for c := range clients {
		wg.Go(func() {
			for i := range perClient {
				records := make([]rec, 0, len(customers))
				for _, customer := range customers {
					records = append(records, rec{id: recordID(c, i, customer), customer: customer})
				}
				if code := p.post(t, request("shop", records...), 10*time.Second); code != http.StatusOK {
					mu.Lock()
					failed = append(failed, code)
					mu.Unlock()
				}
			}
		})
	}
	wg.Wait()
	return failed
}

// expectedRecords returns every record sent, those of customer A and those of
// the other customers.
func expectedRecords(clients, perClient int, customers []string) ([]string, []string, []string) {
	var all, toA, toRest []string
	for c := range clients {
		for i := range perClient {
			for _, customer := range customers {
				id := "shop/e2e/" + recordID(c, i, customer)
				all = append(all, id)
				if customer == "A" {
					toA = append(toA, id)
				} else {
					toRest = append(toRest, id)
				}
			}
		}
	}
	return all, toA, toRest
}

func recordID(client, i int, customer string) string {
	return fmt.Sprintf("client%d-%02d-%s", client, i, customer)
}
