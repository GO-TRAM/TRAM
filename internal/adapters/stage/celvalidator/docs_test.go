// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package celvalidator_test

import (
	"context"
	"strings"
	"testing"

	"github.com/go-playground/validator/v10"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"

	"codeberg.org/eljoth/tram/internal/adapters/stage/celvalidator"
	"codeberg.org/eljoth/tram/internal/core/domain"
	"codeberg.org/eljoth/tram/internal/testutil/otlpgen"
)

const metricsHaveNames = "metrics.resource_metrics.all(r, r.scope_metrics.all(s, s.metrics.all(m, m.name != '')))"

type captureSubscriber struct{ events []*domain.Event }

func (c *captureSubscriber) Name() string { return "capture" }
func (c *captureSubscriber) From() string { return "cel" }
func (c *captureSubscriber) Stop() error  { return nil }
func (c *captureSubscriber) Notify(_ context.Context, e *domain.Event) error {
	c.events = append(c.events, e)
	return nil
}

const (
	tenantUUID = "logs.resource_logs.all(r, r.scope_logs.all(s, s.log_records.all(record, " +
		"record.attributes.exists(a, a.key == 'tenant.id' && " +
		"a.value.string_value.matches('^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')))))"

	validUUID = "82b58687-a6b4-1fb0-8ea7-a73ab9a57970"
)

// withTenants returns one log record per ID, with the ID as tenant.id, or
// without the attribute for an empty ID.
func withTenants(ids ...string) *logspb.LogsData {
	logs := otlpgen.Logs(otlpgen.Shape{Records: len(ids), Resources: 1, Teams: 1})
	for i, record := range logs.GetResourceLogs()[0].GetScopeLogs()[0].GetLogRecords() {
		if ids[i] == "" {
			continue
		}
		record.Attributes = append(record.Attributes, &commonpb.KeyValue{
			Key:   "tenant.id",
			Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: ids[i]}},
		})
	}
	return logs
}

func TestDocumentedBehavior(t *testing.T) {
	t.Parallel()

	shape := otlpgen.Shape{Records: 4, Resources: 2, Teams: 1}

	withoutServiceName := otlpgen.Logs(shape)
	withoutServiceName.ResourceLogs[1].Resource = &resourcepb.Resource{}

	unnamedMetric := otlpgen.Metrics(shape)
	unnamedMetric.ResourceMetrics[0].ScopeMetrics[0].Metrics[0].Name = ""

	invalidTraceID := otlpgen.Traces(shape)
	invalidTraceID.ResourceSpans[0].ScopeSpans[0].Spans[0].TraceId = nil

	cases := []struct {
		name          string
		spec          map[string]any
		event         *domain.Event
		expectReject  bool
		expectRecords int
	}{
		{
			name: "require service.name: accepted",
			spec: map[string]any{
				"logs":        "logs.resource_logs.all(r, r.resource.attributes.exists(a, a.key == 'service.name'))",
				"on_mismatch": "reject",
			},
			event:         &domain.Event{Logs: otlpgen.Logs(shape)},
			expectRecords: 4,
		},
		{
			name: "require service.name: rejected",
			spec: map[string]any{
				"logs":        "logs.resource_logs.all(r, r.resource.attributes.exists(a, a.key == 'service.name'))",
				"on_mismatch": "reject",
			},
			event:        &domain.Event{Logs: withoutServiceName},
			expectReject: true,
		},
		{
			name: "metric names: accepted",
			spec: map[string]any{
				"metrics":     metricsHaveNames,
				"on_mismatch": "drop",
			},
			event:         &domain.Event{Metrics: otlpgen.Metrics(shape)},
			expectRecords: 4,
		},
		{
			name: "metric names: dropped",
			spec: map[string]any{
				"metrics":     metricsHaveNames,
				"on_mismatch": "drop",
			},
			event: &domain.Event{Metrics: unnamedMetric},
		},
		{
			name: "trace IDs: accepted",
			spec: map[string]any{
				"traces":      "traces.resource_spans.all(r, r.scope_spans.all(s, s.spans.all(span, size(span.trace_id) == 16)))",
				"on_mismatch": "reject",
			},
			event:         &domain.Event{Traces: otlpgen.Traces(shape)},
			expectRecords: 4,
		},
		{
			name: "trace IDs: rejected",
			spec: map[string]any{
				"traces":      "traces.resource_spans.all(r, r.scope_spans.all(s, s.spans.all(span, size(span.trace_id) == 16)))",
				"on_mismatch": "reject",
			},
			event:        &domain.Event{Traces: invalidTraceID},
			expectReject: true,
		},
		{
			name:          "tenant.id UUID: accepted",
			spec:          map[string]any{"logs": tenantUUID, "on_mismatch": "reject"},
			event:         &domain.Event{Logs: withTenants(validUUID, "00000000-0000-0000-0000-000000000000")},
			expectRecords: 2,
		},
		{
			name:         "tenant.id UUID: uppercase rejected",
			spec:         map[string]any{"logs": tenantUUID, "on_mismatch": "reject"},
			event:        &domain.Event{Logs: withTenants(validUUID, strings.ToUpper(validUUID))},
			expectReject: true,
		},
		{
			name:         "tenant.id UUID: missing attribute rejected",
			spec:         map[string]any{"logs": tenantUUID, "on_mismatch": "reject"},
			event:        &domain.Event{Logs: withTenants(validUUID, "")},
			expectReject: true,
		},
		{
			// Without ^ and $, matches would find the UUID inside the value.
			name:         "tenant.id UUID: anchors reject a UUID inside a longer value",
			spec:         map[string]any{"logs": tenantUUID, "on_mismatch": "reject"},
			event:        &domain.Event{Logs: withTenants(validUUID, "x."+validUUID)},
			expectReject: true,
		},
		{
			name: "drop removes only the failing signal type",
			spec: map[string]any{
				"metrics":     metricsHaveNames,
				"on_mismatch": "drop",
			},
			event:         &domain.Event{Logs: otlpgen.Logs(shape), Metrics: unnamedMetric},
			expectRecords: 4,
		},
		{
			name: "decision applies to the whole event",
			spec: map[string]any{
				"logs":        "logs.resource_logs.all(r, r.scope_logs.all(s, s.log_records.exists(l, l.attributes.exists(a, a.key == 'team'))))",
				"on_mismatch": "drop",
			},
			event:         &domain.Event{Logs: otlpgen.Logs(shape)},
			expectRecords: 4,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			cel, err := celvalidator.New(domain.StageSpec{Name: "cel", Spec: c.spec}, validator.New())
			require.NoError(t, err)
			sub := &captureSubscriber{}
			require.NoError(t, cel.Subscribe(sub))

			err = cel.Notify(t.Context(), c.event)
			if c.expectReject {
				require.ErrorIs(t, err, domain.ErrEventRejected)
				assert.Empty(t, sub.events)
				return
			}
			require.NoError(t, err)

			records := 0
			for _, e := range sub.events {
				records += e.RecordCount()
			}
			assert.Equal(t, c.expectRecords, records)
		})
	}
}
