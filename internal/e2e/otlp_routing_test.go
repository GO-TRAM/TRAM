// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package e2e_test

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	"google.golang.org/protobuf/proto"

	"github.com/go-tram/tram/internal/core/domain"
)

// tenantEndpoint is an OTLP endpoint of one tenant that records the log
// bodies it receives.
type tenantEndpoint struct {
	*httptest.Server

	mu     sync.Mutex
	bodies []string
}

func newTenantEndpoint(t *testing.T, status int) *tenantEndpoint {
	t.Helper()
	e := &tenantEndpoint{}
	e.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, _ := io.ReadAll(r.Body)
		var logs logspb.LogsData
		_ = proto.Unmarshal(data, &logs)
		e.mu.Lock()
		for _, rl := range logs.GetResourceLogs() {
			for _, sl := range rl.GetScopeLogs() {
				for _, record := range sl.GetLogRecords() {
					e.bodies = append(e.bodies, record.GetBody().GetStringValue())
				}
			}
		}
		e.mu.Unlock()
		w.WriteHeader(status)
	}))
	t.Cleanup(e.Close)
	return e
}

func (e *tenantEndpoint) received() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := append([]string(nil), e.bodies...)
	sort.Strings(out)
	return out
}

// tenantRouting batches concurrent requests and sends each record to the
// endpoint of its customer.
func tenantRouting(t *testing.T, a, b *tenantEndpoint) *pipeline {
	t.Helper()
	expression := fmt.Sprintf("record.attributes.get_attr('customer') == 'A' ? '%s' : '%s'", a.URL, b.URL)
	return newPipeline(t,
		[]domain.StageSpec{batchStage(4, "5s")},
		[]domain.SinkSpec{{Name: "out", Type: "otlp", From: "batch", Spec: map[string]any{
			"url_expression": map[string]any{"logs": expression},
		}}})
}

func TestOTLPSinkRoutesRecordsToTheirTenant(t *testing.T) {
	t.Parallel()

	a, b := newTenantEndpoint(t, http.StatusOK), newTenantEndpoint(t, http.StatusOK)
	p := tenantRouting(t, a, b)

	codes := p.postConcurrently(t,
		request("shop", rec{"s1", "A"}, rec{"s2", "B"}),
		request("cart", rec{"c1", "B"}, rec{"c2", "A"}),
	)
	assert.Equal(t, []int{http.StatusOK, http.StatusOK}, codes)
	assert.Equal(t, []string{"c2", "s1"}, a.received())
	assert.Equal(t, []string{"c1", "s2"}, b.received())
}

// One tenant's endpoint being down fails every request in the batch, so the
// other tenant's data is retried and arrives twice. This is the cost of
// passing deliveries on synchronously without a buffer.
func TestOTLPSinkUnavailableTenantFailsTheBatch(t *testing.T) {
	t.Parallel()

	a, b := newTenantEndpoint(t, http.StatusOK), newTenantEndpoint(t, http.StatusServiceUnavailable)
	p := tenantRouting(t, a, b)

	codes := p.postConcurrently(t,
		request("shop", rec{"s1", "A"}, rec{"s2", "B"}),
		request("cart", rec{"c1", "A"}, rec{"c2", "A"}),
	)
	assert.Equal(t, []int{http.StatusServiceUnavailable, http.StatusServiceUnavailable}, codes,
		"the request with only tenant A's records fails too")
	assert.Equal(t, []string{"c1", "c2", "s1"}, a.received(), "tenant A got its data, which the clients send again")
}
