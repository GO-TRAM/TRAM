// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package cel_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/go-tram/tram/internal/adapters/stage/validation"
	"github.com/go-tram/tram/internal/adapters/stage/validation/cel"
	"github.com/go-tram/tram/internal/core/domain"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracespb "go.opentelemetry.io/proto/otlp/trace/v1"
)

const (
	metricsServiceIsTest = "metrics.resource_metrics.all(resMetric, resMetric.resource.attributes.exists(attr, attr.key == 'service.name' && attr.value.string_value == 'test'))"
	tracesServiceIsTest  = "traces.resource_spans.all(resSpan, resSpan.resource.attributes.exists(attr, attr.key == 'service.name' && attr.value.string_value == 'test'))"
	serviceNameKey       = "service.name"
)

func TestCELValidator(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name        string
		spec        domain.StageSpec
		event       domain.Event
		initErr     error
		validateErr error
	}{
		{
			name: "invalid logs cel",
			spec: domain.StageSpec{
				Spec: map[string]any{
					"logs":        "invalid",
					"on_mismatch": "drop",
				},
			},
			initErr: cel.ErrInvalidLogsExpression,
		},
		{
			name: "valid logs without cel",
			spec: domain.StageSpec{
				Spec: map[string]any{
					"on_mismatch": "drop",
				},
			},
			event: domain.Event{
				Logs: &logspb.LogsData{
					ResourceLogs: []*logspb.ResourceLogs{
						{
							ScopeLogs: []*logspb.ScopeLogs{
								{
									LogRecords: []*logspb.LogRecord{
										{
											Attributes: []*commonpb.KeyValue{
												{
													Key: serviceNameKey,
													Value: &commonpb.AnyValue{
														Value: &commonpb.AnyValue_StringValue{StringValue: "test"},
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "valid logs with cel",
			spec: domain.StageSpec{
				Spec: map[string]any{
					"logs":        "logs.resource_logs.all(resLog, resLog.scope_logs.all(scopeLog, scopeLog.log_records.exists(record, record.attributes.exists(attr, attr.key == 'service.name' && attr.value.string_value == 'test'))))",
					"on_mismatch": "drop",
				},
			},
			event: domain.Event{
				Logs: &logspb.LogsData{
					ResourceLogs: []*logspb.ResourceLogs{
						{
							ScopeLogs: []*logspb.ScopeLogs{
								{
									LogRecords: []*logspb.LogRecord{
										{
											Attributes: []*commonpb.KeyValue{
												{
													Key: serviceNameKey,
													Value: &commonpb.AnyValue{
														Value: &commonpb.AnyValue_StringValue{StringValue: "test"},
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "drop logs",
			spec: domain.StageSpec{
				Spec: map[string]any{
					"logs":        "logs.resource_logs.all(resLog, resLog.scope_logs.all(scopeLog, scopeLog.log_records.exists(record, record.attributes.exists(attr, attr.key == 'service.name' && attr.value.string_value == 'test2'))))",
					"on_mismatch": "drop",
				},
			},
			event: domain.Event{
				Logs: &logspb.LogsData{
					ResourceLogs: []*logspb.ResourceLogs{
						{
							ScopeLogs: []*logspb.ScopeLogs{
								{
									LogRecords: []*logspb.LogRecord{
										{
											Attributes: []*commonpb.KeyValue{
												{
													Key: serviceNameKey,
													Value: &commonpb.AnyValue{
														Value: &commonpb.AnyValue_StringValue{StringValue: "test"},
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "reject logs",
			spec: domain.StageSpec{
				Spec: map[string]any{
					"logs":        "logs.resource_logs.all(resLog, resLog.scope_logs.all(scopeLog, scopeLog.log_records.exists(record, record.attributes.exists(attr, attr.key == 'service.name' && attr.value.string_value == 'test'))))",
					"on_mismatch": "reject",
				},
			},
			event: domain.Event{
				Logs: &logspb.LogsData{
					ResourceLogs: []*logspb.ResourceLogs{
						{
							ScopeLogs: []*logspb.ScopeLogs{
								{
									LogRecords: []*logspb.LogRecord{
										{
											Attributes: []*commonpb.KeyValue{
												{
													Key: serviceNameKey,
													Value: &commonpb.AnyValue{
														Value: &commonpb.AnyValue_StringValue{StringValue: "test2"},
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			validateErr: validation.ErrLogsRejected,
		},
		{
			name: "invalid metrics cel",
			spec: domain.StageSpec{
				Spec: map[string]any{
					"metrics":     "invalid",
					"on_mismatch": "drop",
				},
			},
			initErr: cel.ErrInvalidMetricsExpression,
		},
		{
			name: "valid metrics without cel",
			spec: domain.StageSpec{
				Spec: map[string]any{
					"on_mismatch": "drop",
				},
			},
			event: domain.Event{
				Metrics: &metricspb.MetricsData{
					ResourceMetrics: []*metricspb.ResourceMetrics{
						{ScopeMetrics: []*metricspb.ScopeMetrics{{Metrics: []*metricspb.Metric{}}}},
					},
				},
			},
		},
		{
			name: "valid metrics with cel",
			spec: domain.StageSpec{
				Spec: map[string]any{
					"metrics":     metricsServiceIsTest,
					"on_mismatch": "drop",
				},
			},
			event: domain.Event{
				Metrics: &metricspb.MetricsData{
					ResourceMetrics: []*metricspb.ResourceMetrics{
						{
							Resource: &resourcepb.Resource{
								Attributes: []*commonpb.KeyValue{
									{
										Key: serviceNameKey,
										Value: &commonpb.AnyValue{
											Value: &commonpb.AnyValue_StringValue{StringValue: "test"},
										},
									},
								},
							},
							ScopeMetrics: []*metricspb.ScopeMetrics{{Metrics: []*metricspb.Metric{}}},
						},
					},
				},
			},
		},
		{
			name: "drop metrics",
			spec: domain.StageSpec{
				Spec: map[string]any{
					"metrics":     metricsServiceIsTest,
					"on_mismatch": "drop",
				},
			},
			event: domain.Event{
				Metrics: &metricspb.MetricsData{
					ResourceMetrics: []*metricspb.ResourceMetrics{
						{
							Resource: &resourcepb.Resource{
								Attributes: []*commonpb.KeyValue{
									{
										Key: serviceNameKey,
										Value: &commonpb.AnyValue{
											Value: &commonpb.AnyValue_StringValue{StringValue: "test2"},
										},
									},
								},
							},
							ScopeMetrics: []*metricspb.ScopeMetrics{{Metrics: []*metricspb.Metric{}}},
						},
					},
				},
			},
		},
		{
			name: "reject metrics",
			spec: domain.StageSpec{
				Spec: map[string]any{
					"metrics":     metricsServiceIsTest,
					"on_mismatch": "reject",
				},
			},
			event: domain.Event{
				Metrics: &metricspb.MetricsData{
					ResourceMetrics: []*metricspb.ResourceMetrics{
						{
							Resource: &resourcepb.Resource{
								Attributes: []*commonpb.KeyValue{
									{
										Key: serviceNameKey,
										Value: &commonpb.AnyValue{
											Value: &commonpb.AnyValue_StringValue{StringValue: "test2"},
										},
									},
								},
							},
							ScopeMetrics: []*metricspb.ScopeMetrics{{Metrics: []*metricspb.Metric{}}},
						},
					},
				},
			},
			validateErr: validation.ErrMetricsRejected,
		},
		{
			name: "invalid traces cel",
			spec: domain.StageSpec{
				Spec: map[string]any{
					"traces":      "invalid",
					"on_mismatch": "drop",
				},
			},
			initErr: cel.ErrInvalidTracesExpression,
		},
		{
			name: "valid traces without cel",
			spec: domain.StageSpec{
				Spec: map[string]any{
					"on_mismatch": "drop",
				},
			},
			event: domain.Event{
				Traces: &tracespb.TracesData{
					ResourceSpans: []*tracespb.ResourceSpans{
						{ScopeSpans: []*tracespb.ScopeSpans{{Spans: []*tracespb.Span{}}}},
					},
				},
			},
		},
		{
			name: "valid traces with cel",
			spec: domain.StageSpec{
				Spec: map[string]any{
					"traces":      tracesServiceIsTest,
					"on_mismatch": "drop",
				},
			},
			event: domain.Event{
				Traces: &tracespb.TracesData{
					ResourceSpans: []*tracespb.ResourceSpans{
						{
							Resource: &resourcepb.Resource{
								Attributes: []*commonpb.KeyValue{
									{
										Key: serviceNameKey,
										Value: &commonpb.AnyValue{
											Value: &commonpb.AnyValue_StringValue{StringValue: "test"},
										},
									},
								},
							},
							ScopeSpans: []*tracespb.ScopeSpans{{Spans: []*tracespb.Span{}}},
						},
					},
				},
			},
		},
		{
			name: "drop traces",
			spec: domain.StageSpec{
				Spec: map[string]any{
					"traces":      tracesServiceIsTest,
					"on_mismatch": "drop",
				},
			},
			event: domain.Event{
				Traces: &tracespb.TracesData{
					ResourceSpans: []*tracespb.ResourceSpans{
						{
							Resource: &resourcepb.Resource{
								Attributes: []*commonpb.KeyValue{
									{
										Key: serviceNameKey,
										Value: &commonpb.AnyValue{
											Value: &commonpb.AnyValue_StringValue{StringValue: "test2"},
										},
									},
								},
							},
							ScopeSpans: []*tracespb.ScopeSpans{{Spans: []*tracespb.Span{}}},
						},
					},
				},
			},
		},
		{
			name: "reject traces",
			spec: domain.StageSpec{
				Spec: map[string]any{
					"traces":      tracesServiceIsTest,
					"on_mismatch": "reject",
				},
			},
			event: domain.Event{
				Traces: &tracespb.TracesData{
					ResourceSpans: []*tracespb.ResourceSpans{
						{
							Resource: &resourcepb.Resource{
								Attributes: []*commonpb.KeyValue{
									{
										Key: serviceNameKey,
										Value: &commonpb.AnyValue{
											Value: &commonpb.AnyValue_StringValue{StringValue: "test2"},
										},
									},
								},
							},
							ScopeSpans: []*tracespb.ScopeSpans{{Spans: []*tracespb.Span{}}},
						},
					},
				},
			},
			validateErr: validation.ErrTracesRejected,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			stage, err := newStage(c.spec.Name, c.spec.Spec)
			require.ErrorIs(t, err, c.initErr)
			if c.initErr != nil {
				return
			}
			assert.ErrorIs(t, stage.Notify(t.Context(), &c.event), c.validateErr)
		})
	}
}

func TestCELValidatorEvaluationErrorRejects(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		expression string
	}{
		{name: "missing list index", expression: "logs.resource_logs[0].resource.attributes.size() > 0"},
		{name: "result is not a bool", expression: "logs.resource_logs.size()"},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			stage, err := newStage("", map[string]any{
				"logs":        c.expression,
				"on_mismatch": "drop",
			})
			require.NoError(t, err)

			err = stage.Notify(t.Context(), &domain.Event{Logs: &logspb.LogsData{}})
			require.ErrorIs(t, err, domain.ErrEventRejected)
			assert.ErrorIs(t, err, cel.ErrEvaluation)
		})
	}
}

func TestCELValidatorRejectsUnknownVariable(t *testing.T) {
	t.Parallel()

	_, err := newStage("", map[string]any{
		"logs":        "event.Logs != null",
		"on_mismatch": "reject",
	})
	assert.ErrorIs(t, err, cel.ErrInvalidLogsExpression)
}

func TestCELValidatorDropDoesNotMutateEvent(t *testing.T) {
	t.Parallel()

	stage, err := newStage("", map[string]any{
		"logs":        "false",
		"on_mismatch": "drop",
	})
	assert.NoError(t, err)

	logs := &logspb.LogsData{}
	metrics := &metricspb.MetricsData{}
	event := &domain.Event{Logs: logs, Metrics: metrics}

	assert.NoError(t, stage.Notify(t.Context(), event))
	assert.Same(t, logs, event.Logs)
	assert.Same(t, metrics, event.Metrics)
}
