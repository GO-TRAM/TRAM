// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package celvalidator_test

import (
	"testing"

	"github.com/go-playground/validator/v10"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"codeberg.org/eljoth/tram/internal/adapters/stage/celvalidator"
	"codeberg.org/eljoth/tram/internal/core/domain"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracespb "go.opentelemetry.io/proto/otlp/trace/v1"
)

func TestCELValidator(t *testing.T) {
	t.Parallel()

	validator := validator.New()
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
			initErr:     celvalidator.ErrCELInvalidLogsExpression,
			validateErr: celvalidator.ErrCELEnvironmenInitialization,
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
													Key: "service.name",
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
													Key: "service.name",
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
													Key: "service.name",
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
													Key: "service.name",
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
			validateErr: celvalidator.ErrCELRejectingLogs,
		},
		{
			name: "invalid metrics cel",
			spec: domain.StageSpec{
				Spec: map[string]any{
					"metrics":     "invalid",
					"on_mismatch": "drop",
				},
			},
			initErr:     celvalidator.ErrCELInvalidMetricsExpression,
			validateErr: celvalidator.ErrCELEnvironmenInitialization,
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
					"metrics":     "metrics.resource_metrics.all(resMetric, resMetric.resource.attributes.exists(attr, attr.key == 'service.name' && attr.value.string_value == 'test'))",
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
										Key: "service.name",
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
					"metrics":     "metrics.resource_metrics.all(resMetric, resMetric.resource.attributes.exists(attr, attr.key == 'service.name' && attr.value.string_value == 'test'))",
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
										Key: "service.name",
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
					"metrics":     "metrics.resource_metrics.all(resMetric, resMetric.resource.attributes.exists(attr, attr.key == 'service.name' && attr.value.string_value == 'test'))",
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
										Key: "service.name",
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
			validateErr: celvalidator.ErrCELRejectingMetrics,
		},
		{
			name: "invalid traces cel",
			spec: domain.StageSpec{
				Spec: map[string]any{
					"traces":      "invalid",
					"on_mismatch": "drop",
				},
			},
			initErr:     celvalidator.ErrCELInvalidTracesExpression,
			validateErr: celvalidator.ErrCELEnvironmenInitialization,
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
					"traces":      "traces.resource_spans.all(resSpan, resSpan.resource.attributes.exists(attr, attr.key == 'service.name' && attr.value.string_value == 'test'))",
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
										Key: "service.name",
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
					"traces":      "traces.resource_spans.all(resSpan, resSpan.resource.attributes.exists(attr, attr.key == 'service.name' && attr.value.string_value == 'test'))",
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
										Key: "service.name",
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
					"traces":      "traces.resource_spans.all(resSpan, resSpan.resource.attributes.exists(attr, attr.key == 'service.name' && attr.value.string_value == 'test'))",
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
										Key: "service.name",
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
			validateErr: celvalidator.ErrCELRejectingTraces,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			cel, err := celvalidator.New(c.spec, validator)
			require.ErrorIs(t, err, c.initErr)
			assert.ErrorIs(t, cel.Notify(t.Context(), &c.event), c.validateErr)
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

			cel, err := celvalidator.New(domain.StageSpec{
				Spec: map[string]any{
					"logs":        c.expression,
					"on_mismatch": "drop",
				},
			}, validator.New())
			require.NoError(t, err)

			err = cel.Notify(t.Context(), &domain.Event{Logs: &logspb.LogsData{}})
			require.ErrorIs(t, err, domain.ErrEventRejected)
			assert.ErrorIs(t, err, celvalidator.ErrCELEvaluation)
		})
	}
}

func TestCELValidatorRejectsUnknownVariable(t *testing.T) {
	t.Parallel()

	_, err := celvalidator.New(domain.StageSpec{
		Spec: map[string]any{
			"logs":        "event.Logs != null",
			"on_mismatch": "reject",
		},
	}, validator.New())
	assert.ErrorIs(t, err, celvalidator.ErrCELInvalidLogsExpression)
}

func TestCELValidatorDropDoesNotMutateEvent(t *testing.T) {
	t.Parallel()

	cel, err := celvalidator.New(domain.StageSpec{
		Spec: map[string]any{
			"logs":        "false",
			"on_mismatch": "drop",
		},
	}, validator.New())
	assert.NoError(t, err)

	logs := &logspb.LogsData{}
	metrics := &metricspb.MetricsData{}
	event := &domain.Event{Logs: logs, Metrics: metrics}

	assert.NoError(t, cel.Notify(t.Context(), event))
	assert.Same(t, logs, event.Logs)
	assert.Same(t, metrics, event.Metrics)
}
