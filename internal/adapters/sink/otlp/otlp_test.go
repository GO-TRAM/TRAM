// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package otlp_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/go-playground/validator/v10"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"

	"codeberg.org/tram/tram/internal/adapters/sink/otlp"
	"codeberg.org/tram/tram/internal/core/domain"
)

const (
	logsPath     = "/v1/logs"
	testSource   = "test-source"
	testEndpoint = "http://localhost:4318"
	customHeader = "X-Custom"
)

func TestNew(t *testing.T) {
	t.Parallel()

	t.Run("valid configuration", func(t *testing.T) {
		t.Parallel()

		sink, err := otlp.New(domain.SinkSpec{
			Name: "test-otlp",
			From: testSource,
			Type: otlp.Type,
			Spec: map[string]any{
				"url": testEndpoint,
			},
		}, validator.New(), slog.New(slog.DiscardHandler))

		require.NoError(t, err)
		assert.NotNil(t, sink)
		assert.Equal(t, "test-otlp", sink.Name())
		assert.Equal(t, testSource, sink.From())
		assert.NoError(t, sink.Stop())
	})

	t.Run("valid configuration with timeout", func(t *testing.T) {
		t.Parallel()

		sink, err := otlp.New(domain.SinkSpec{
			Name: "test-otlp-timeout",
			From: testSource,
			Type: otlp.Type,
			Spec: map[string]any{
				"url":     testEndpoint,
				"timeout": "5s",
			},
		}, validator.New(), slog.New(slog.DiscardHandler))

		require.NoError(t, err)
		assert.NotNil(t, sink)
		assert.NoError(t, sink.Stop())
	})

	t.Run("valid configuration with headers", func(t *testing.T) {
		t.Parallel()

		sink, err := otlp.New(domain.SinkSpec{
			Name: "test-otlp-headers",
			From: testSource,
			Type: otlp.Type,
			Spec: map[string]any{
				"url": testEndpoint,
				"headers": map[string]string{
					"Authorization": "Bearer token",
					customHeader:    "value",
				},
			},
		}, validator.New(), slog.New(slog.DiscardHandler))

		require.NoError(t, err)
		assert.NotNil(t, sink)
		assert.NoError(t, sink.Stop())
	})

	t.Run("valid configuration with insecure", func(t *testing.T) {
		t.Parallel()

		sink, err := otlp.New(domain.SinkSpec{
			Name: "test-otlp-insecure",
			From: testSource,
			Type: otlp.Type,
			Spec: map[string]any{
				"url":      "https://localhost:4318",
				"insecure": true,
			},
		}, validator.New(), slog.New(slog.DiscardHandler))

		require.NoError(t, err)
		assert.NotNil(t, sink)
		assert.NoError(t, sink.Stop())
	})

	t.Run("missing required URL", func(t *testing.T) {
		t.Parallel()

		_, err := otlp.New(domain.SinkSpec{
			Name: "test-otlp-missing-url",
			From: testSource,
			Type: otlp.Type,
			Spec: map[string]any{},
		}, validator.New(), slog.New(slog.DiscardHandler))

		require.Error(t, err)
	})

	t.Run("invalid timeout", func(t *testing.T) {
		t.Parallel()

		_, err := otlp.New(domain.SinkSpec{
			Name: "test-otlp-bad-timeout",
			From: testSource,
			Type: otlp.Type,
			Spec: map[string]any{
				"url":     testEndpoint,
				"timeout": "invalid",
			},
		}, validator.New(), slog.New(slog.DiscardHandler))

		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid timeout")
	})
}

func TestNotify(t *testing.T) {
	t.Parallel()

	t.Run("successful logs delivery", func(t *testing.T) {
		t.Parallel()

		var receivedBody []byte
		var receivedHeaders http.Header
		var mu sync.Mutex

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			defer mu.Unlock()
			receivedHeaders = r.Header.Clone()
			body, _ := io.ReadAll(r.Body)
			receivedBody = body
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		sink, err := otlp.New(domain.SinkSpec{
			Name: "test-otlp-success",
			From: testSource,
			Type: otlp.Type,
			Spec: map[string]any{
				"url": server.URL,
			},
		}, validator.New(), slog.New(slog.DiscardHandler))
		require.NoError(t, err)
		defer sink.Stop()

		validLogs, err := proto.Marshal(&logspb.LogsData{ResourceLogs: []*logspb.ResourceLogs{{}}})
		require.NoError(t, err)

		event := &domain.Event{
			Logs: &logspb.LogsData{},
		}
		err = proto.Unmarshal(validLogs, event.Logs)
		require.NoError(t, err)

		err = sink.Notify(context.Background(), event)
		require.NoError(t, err)

		mu.Lock()
		defer mu.Unlock()
		assert.Equal(t, "application/x-protobuf", receivedHeaders.Get("Content-Type"))
		assert.NotEmpty(t, receivedBody)

		unmarshaled := &logspb.LogsData{}
		err = proto.Unmarshal(receivedBody, unmarshaled)
		require.NoError(t, err)
		assert.Len(t, unmarshaled.GetResourceLogs(), 1)
	})

	t.Run("successful metrics delivery", func(t *testing.T) {
		t.Parallel()

		var receivedBody []byte
		var mu sync.Mutex

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			defer mu.Unlock()
			body, _ := io.ReadAll(r.Body)
			receivedBody = body
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		sink, err := otlp.New(domain.SinkSpec{
			Name: "test-otlp-metrics",
			From: testSource,
			Type: otlp.Type,
			Spec: map[string]any{
				"url": server.URL,
			},
		}, validator.New(), slog.New(slog.DiscardHandler))
		require.NoError(t, err)
		defer sink.Stop()

		validMetrics, err := proto.Marshal(&metricspb.MetricsData{ResourceMetrics: []*metricspb.ResourceMetrics{{}}})
		require.NoError(t, err)

		event := &domain.Event{
			Metrics: &metricspb.MetricsData{},
		}
		err = proto.Unmarshal(validMetrics, event.Metrics)
		require.NoError(t, err)

		err = sink.Notify(context.Background(), event)
		require.NoError(t, err)

		mu.Lock()
		defer mu.Unlock()
		assert.NotEmpty(t, receivedBody)

		unmarshaled := &metricspb.MetricsData{}
		err = proto.Unmarshal(receivedBody, unmarshaled)
		require.NoError(t, err)
		assert.Len(t, unmarshaled.GetResourceMetrics(), 1)
	})

	t.Run("event with several signals", func(t *testing.T) {
		t.Parallel()

		received := map[string]int{}
		var mu sync.Mutex

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			defer mu.Unlock()
			body, _ := io.ReadAll(r.Body)
			var event *domain.Event
			switch r.URL.Path {
			case logsPath:
				event, _ = domain.UnmarshalOTLPData(domain.SignalLogs, body)
			case "/v1/traces":
				event, _ = domain.UnmarshalOTLPData(domain.SignalTraces, body)
			}
			received[r.URL.Path] += event.RecordCount()
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		sink, err := otlp.New(domain.SinkSpec{
			Name: "test-otlp-several-signals",
			From: testSource,
			Type: otlp.Type,
			Spec: map[string]any{
				"url": server.URL,
			},
		}, validator.New(), slog.New(slog.DiscardHandler))
		require.NoError(t, err)
		defer sink.Stop()

		event := &domain.Event{
			Logs: &logspb.LogsData{ResourceLogs: []*logspb.ResourceLogs{{
				ScopeLogs: []*logspb.ScopeLogs{{LogRecords: []*logspb.LogRecord{{}, {}}}},
			}}},
			Traces: &tracepb.TracesData{ResourceSpans: []*tracepb.ResourceSpans{{
				ScopeSpans: []*tracepb.ScopeSpans{{Spans: []*tracepb.Span{{}, {}, {}}}},
			}}},
		}

		err = sink.Notify(context.Background(), event)
		require.NoError(t, err)

		mu.Lock()
		defer mu.Unlock()
		assert.Equal(t, map[string]int{logsPath: 2, "/v1/traces": 3}, received)
	})

	t.Run("server error response", func(t *testing.T) {
		t.Parallel()

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "Bad Request", http.StatusBadRequest)
		}))
		defer server.Close()

		sink, err := otlp.New(domain.SinkSpec{
			Name: "test-otlp-error",
			From: testSource,
			Type: otlp.Type,
			Spec: map[string]any{
				"url": server.URL,
			},
		}, validator.New(), slog.New(slog.DiscardHandler))
		require.NoError(t, err)
		defer sink.Stop()

		validLogs, err := proto.Marshal(&logspb.LogsData{ResourceLogs: []*logspb.ResourceLogs{{}}})
		require.NoError(t, err)

		event := &domain.Event{
			Logs: &logspb.LogsData{},
		}
		err = proto.Unmarshal(validLogs, event.Logs)
		require.NoError(t, err)

		err = sink.Notify(context.Background(), event)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "400")
	})

	t.Run("nil event", func(t *testing.T) {
		t.Parallel()

		sink, err := otlp.New(domain.SinkSpec{
			Name: "test-otlp-nil",
			From: testSource,
			Type: otlp.Type,
			Spec: map[string]any{
				"url": testEndpoint,
			},
		}, validator.New(), slog.New(slog.DiscardHandler))
		require.NoError(t, err)
		defer sink.Stop()

		err = sink.Notify(context.Background(), nil)
		require.NoError(t, err)
	})

	t.Run("empty event", func(t *testing.T) {
		t.Parallel()

		sink, err := otlp.New(domain.SinkSpec{
			Name: "test-otlp-empty",
			From: testSource,
			Type: otlp.Type,
			Spec: map[string]any{
				"url": testEndpoint,
			},
		}, validator.New(), slog.New(slog.DiscardHandler))
		require.NoError(t, err)
		defer sink.Stop()

		err = sink.Notify(context.Background(), &domain.Event{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no OTLP data type set")
	})

	t.Run("URL with trailing slash", func(t *testing.T) {
		t.Parallel()

		var receivedPath string
		var mu sync.Mutex

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			defer mu.Unlock()
			receivedPath = r.URL.Path
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		sink, err := otlp.New(domain.SinkSpec{
			Name: "test-otlp-trailing-slash",
			From: testSource,
			Type: otlp.Type,
			Spec: map[string]any{
				"url": server.URL + "/",
			},
		}, validator.New(), slog.New(slog.DiscardHandler))
		require.NoError(t, err)
		defer sink.Stop()

		validLogs, err := proto.Marshal(&logspb.LogsData{ResourceLogs: []*logspb.ResourceLogs{{}}})
		require.NoError(t, err)

		event := &domain.Event{
			Logs: &logspb.LogsData{},
		}
		err = proto.Unmarshal(validLogs, event.Logs)
		require.NoError(t, err)

		err = sink.Notify(context.Background(), event)
		require.NoError(t, err)

		mu.Lock()
		defer mu.Unlock()
		assert.Equal(t, logsPath, receivedPath)
	})

	t.Run("custom headers", func(t *testing.T) {
		t.Parallel()

		var receivedHeaders http.Header
		var mu sync.Mutex

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			defer mu.Unlock()
			receivedHeaders = r.Header.Clone()
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		sink, err := otlp.New(domain.SinkSpec{
			Name: "test-otlp-custom-headers",
			From: testSource,
			Type: otlp.Type,
			Spec: map[string]any{
				"url": server.URL,
				"headers": map[string]string{
					"Authorization": "Bearer secret-token",
					customHeader:    "custom-value",
				},
			},
		}, validator.New(), slog.New(slog.DiscardHandler))
		require.NoError(t, err)
		defer sink.Stop()

		validLogs, err := proto.Marshal(&logspb.LogsData{ResourceLogs: []*logspb.ResourceLogs{{}}})
		require.NoError(t, err)

		event := &domain.Event{
			Logs: &logspb.LogsData{},
		}
		err = proto.Unmarshal(validLogs, event.Logs)
		require.NoError(t, err)

		err = sink.Notify(context.Background(), event)
		require.NoError(t, err)

		mu.Lock()
		defer mu.Unlock()
		assert.Equal(t, "Bearer secret-token", receivedHeaders.Get("Authorization"))
		assert.Equal(t, "custom-value", receivedHeaders.Get(customHeader))
	})
}

func TestStop(t *testing.T) {
	t.Parallel()

	sink, err := otlp.New(domain.SinkSpec{
		Name: "test-otlp-stop",
		From: testSource,
		Type: otlp.Type,
		Spec: map[string]any{
			"url": testEndpoint,
		},
	}, validator.New(), slog.New(slog.DiscardHandler))
	require.NoError(t, err)

	assert.NoError(t, sink.Stop())
	assert.NoError(t, sink.Stop())
}

func TestNotifyClassifiesStatus(t *testing.T) {
	t.Parallel()

	cases := []struct {
		status   int
		rejected bool
	}{
		{status: http.StatusBadRequest, rejected: true},
		{status: http.StatusUnauthorized, rejected: true},
		{status: http.StatusRequestEntityTooLarge, rejected: true},
		{status: http.StatusRequestTimeout, rejected: false},
		{status: http.StatusTooManyRequests, rejected: false},
		{status: http.StatusInternalServerError, rejected: false},
		{status: http.StatusServiceUnavailable, rejected: false},
	}

	for _, c := range cases {
		t.Run(http.StatusText(c.status), func(t *testing.T) {
			t.Parallel()

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(c.status)
			}))
			defer server.Close()

			sink, err := otlp.New(domain.SinkSpec{
				Name: "test-otlp-status",
				From: testSource,
				Type: otlp.Type,
				Spec: map[string]any{"url": server.URL},
			}, validator.New(), slog.New(slog.DiscardHandler))
			require.NoError(t, err)
			defer sink.Stop()

			err = sink.Notify(t.Context(), &domain.Event{Logs: &logspb.LogsData{}})
			require.Error(t, err)
			assert.Equal(t, c.rejected, errors.Is(err, domain.ErrEventRejected))
		})
	}
}

func TestNewRejectsNonPositiveTimeout(t *testing.T) {
	t.Parallel()

	for _, timeout := range []string{"0s", "-1s"} {
		_, err := otlp.New(domain.SinkSpec{
			Name: "test-otlp-timeout",
			From: testSource,
			Type: otlp.Type,
			Spec: map[string]any{"url": testEndpoint, "timeout": timeout},
		}, validator.New(), slog.New(slog.DiscardHandler))
		require.ErrorContains(t, err, "not positive", timeout)
	}
}
