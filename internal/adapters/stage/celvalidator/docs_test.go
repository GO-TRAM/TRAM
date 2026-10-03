package celvalidator_test

import (
	"context"
	"testing"

	"github.com/go-playground/validator/v10"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"

	"codeberg.org/eljoth/tram/internal/adapters/stage/celvalidator"
	"codeberg.org/eljoth/tram/internal/core/domain"
	"codeberg.org/eljoth/tram/internal/testutil/otlpgen"
)

type captureSubscriber struct{ events []*domain.Event }

func (c *captureSubscriber) Name() string { return "capture" }
func (c *captureSubscriber) From() string { return "cel" }
func (c *captureSubscriber) Stop() error  { return nil }
func (c *captureSubscriber) Notify(_ context.Context, e *domain.Event) error {
	c.events = append(c.events, e)
	return nil
}

// TestDocumentedBehavior keeps the examples and behavior described in
// COMPONENTS.md correct.
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
				"metrics":     "metrics.resource_metrics.all(r, r.scope_metrics.all(s, s.metrics.all(m, m.name != '')))",
				"on_mismatch": "drop",
			},
			event:         &domain.Event{Metrics: otlpgen.Metrics(shape)},
			expectRecords: 4,
		},
		{
			name: "metric names: dropped",
			spec: map[string]any{
				"metrics":     "metrics.resource_metrics.all(r, r.scope_metrics.all(s, s.metrics.all(m, m.name != '')))",
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
			name: "drop removes only the failing signal type",
			spec: map[string]any{
				"metrics":     "metrics.resource_metrics.all(r, r.scope_metrics.all(s, s.metrics.all(m, m.name != '')))",
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
