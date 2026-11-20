// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package core_test

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"codeberg.org/tram/tram/internal/adapters/stage"
	"codeberg.org/tram/tram/internal/core"
	"codeberg.org/tram/tram/internal/core/domain"
)

func testLogger() *slog.Logger { return slog.New(slog.DiscardHandler) }

// logEvent builds an OTLP JSON logs object with a single record.
func otlpLogs(body string) map[string]any {
	return map[string]any{
		"logs": map[string]any{
			"resourceLogs": []any{
				map[string]any{
					"scopeLogs": []any{
						map[string]any{
							"logRecords": []any{
								map[string]any{"body": map[string]any{"stringValue": body}},
							},
						},
					},
				},
			},
		},
	}
}

func testStream(stages []domain.StageSpec, tests []domain.StreamTest) domain.StreamSpec {
	from := "in"
	for i := range stages {
		stages[i].From = from
		from = stages[i].Name
	}
	return domain.StreamSpec{
		Name:   "test-stream",
		Source: domain.SourceSpec{Name: "in", Type: "test"},
		Stages: stages,
		Sinks:  []domain.SinkSpec{{Name: "out", Type: "test", From: from}},
		Tests:  tests,
	}
}

func runTests(t *testing.T, spec domain.StreamSpec) []core.TestResult {
	t.Helper()
	results, err := core.RunStreamTests(t.Context(), spec, stage.NewFactory(testLogger()), testLogger())
	require.NoError(t, err)
	return results
}

func TestRunStreamTestsPassthrough(t *testing.T) {
	t.Parallel()

	spec := testStream(nil, []domain.StreamTest{{
		Name:   "forwards the event",
		Input:  otlpLogs("hello"),
		Expect: map[string]any{"out": otlpLogs("hello")},
	}})

	results := runTests(t, spec)
	require.Len(t, results, 1)
	assert.True(t, results[0].Passed, "problems: %v", results[0].Problems)
}

func TestRunStreamTestsDrop(t *testing.T) {
	t.Parallel()

	spec := testStream([]domain.StageSpec{{
		Name: "filter",
		Type: "filter",
		Spec: map[string]any{"driver": "cel", "cel": map[string]any{"logs": "false"}},
	}}, []domain.StreamTest{{
		Name:   "drops the event",
		Input:  otlpLogs("hello"),
		Expect: map[string]any{"out": "drop"},
	}})

	results := runTests(t, spec)
	require.Len(t, results, 1)
	assert.True(t, results[0].Passed, "problems: %v", results[0].Problems)
}

func TestRunStreamTestsReject(t *testing.T) {
	t.Parallel()

	spec := testStream([]domain.StageSpec{{
		Name: "validate",
		Type: "validation",
		Spec: map[string]any{"driver": "cel", "on_mismatch": "reject", "cel": map[string]any{"logs": "false"}},
	}}, []domain.StreamTest{{
		Name:   "rejects the event",
		Input:  otlpLogs("hello"),
		Expect: map[string]any{"out": "reject"},
	}})

	results := runTests(t, spec)
	require.Len(t, results, 1)
	assert.True(t, results[0].Passed, "problems: %v", results[0].Problems)
}

func TestRunStreamTestsMismatch(t *testing.T) {
	t.Parallel()

	spec := testStream(nil, []domain.StreamTest{{
		Name:   "wrong expectation",
		Input:  otlpLogs("hello"),
		Expect: map[string]any{"out": otlpLogs("goodbye")},
	}})

	results := runTests(t, spec)
	require.Len(t, results, 1)
	assert.False(t, results[0].Passed)
	require.Len(t, results[0].Problems, 1)
	assert.Contains(t, results[0].Problems[0], "out")
}

func TestRunStreamTestsUnexpectedDelivery(t *testing.T) {
	t.Parallel()

	spec := testStream(nil, []domain.StreamTest{{
		Name:   "sink without expectation must receive nothing",
		Input:  otlpLogs("hello"),
		Expect: map[string]any{},
	}})

	results := runTests(t, spec)
	require.Len(t, results, 1)
	assert.False(t, results[0].Passed)
	require.Len(t, results[0].Problems, 1)
	assert.Contains(t, results[0].Problems[0], "expected drop")
}

func TestRunStreamTestsInvalidInput(t *testing.T) {
	t.Parallel()

	spec := testStream(nil, []domain.StreamTest{{
		Name:   "invalid input",
		Input:  map[string]any{"unknown": map[string]any{}},
		Expect: map[string]any{"out": "drop"},
	}})

	results := runTests(t, spec)
	require.Len(t, results, 1)
	assert.False(t, results[0].Passed)
	require.Len(t, results[0].Problems, 1)
	assert.Contains(t, results[0].Problems[0], "invalid input")
}
