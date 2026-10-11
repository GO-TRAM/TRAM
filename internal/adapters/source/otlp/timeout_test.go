// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package otlp_test

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	"google.golang.org/protobuf/proto"

	"codeberg.org/eljoth/tram/internal/adapters/source/base"
	"codeberg.org/eljoth/tram/internal/adapters/source/otlp"
	"codeberg.org/eljoth/tram/internal/core/domain"
)

type slowSubscriber struct {
	delay  time.Duration
	ctxErr chan error
}

func (s *slowSubscriber) Name() string { return "slow" }
func (s *slowSubscriber) From() string { return "otlp" }
func (s *slowSubscriber) Stop() error  { return nil }
func (s *slowSubscriber) Notify(ctx context.Context, _ *domain.Event) error {
	select {
	case <-time.After(s.delay):
		return nil
	case <-ctx.Done():
		s.ctxErr <- ctx.Err()
		return ctx.Err()
	}
}

func logsBody(t *testing.T) []byte {
	t.Helper()
	body, err := proto.Marshal(&logspb.LogsData{ResourceLogs: []*logspb.ResourceLogs{{}}})
	require.NoError(t, err)
	return body
}

func freePort(t *testing.T) int {
	t.Helper()
	var lc net.ListenConfig
	l, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := l.Addr().(*net.TCPAddr).Port
	require.NoError(t, l.Close())
	return port
}

func newOTLP(t *testing.T, spec map[string]any) (*otlp.OTLP, error) {
	t.Helper()
	spec["http_port"] = freePort(t)
	return otlp.New(
		domain.SourceSpec{Name: "otlp", Type: otlp.Type, Spec: spec},
		validator.New(),
		slog.New(slog.DiscardHandler),
	)
}

func TestDeliveryTimeoutCancelsDelivery(t *testing.T) {
	t.Parallel()

	src, err := base.New[otlp.Spec]("otlp", map[string]any{"http_port": 1}, validator.New())
	require.NoError(t, err)
	sub := &slowSubscriber{delay: time.Hour, ctxErr: make(chan error, 1)}
	require.NoError(t, src.Subscribe(sub))
	o := otlp.NewWithoutListener(src, 50*time.Millisecond)

	start := time.Now()
	w := httptest.NewRecorder()
	otlp.HandleLogs(o, w, httptest.NewRequest(http.MethodPost, "/v1/logs", bytes.NewReader(logsBody(t))))

	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
	assert.Less(t, time.Since(start), time.Second)
	assert.ErrorIs(t, <-sub.ctxErr, context.DeadlineExceeded, "the delivery is cancelled")
}

func TestSlowDeliveryIsAnswered(t *testing.T) {
	t.Parallel()

	o, err := newOTLP(t, map[string]any{"delivery_timeout": "5s"})
	require.NoError(t, err)
	otlp.SetResponseWriteTimeout(o, 100*time.Millisecond)
	require.NoError(t, o.Subscribe(&slowSubscriber{delay: 300 * time.Millisecond, ctxErr: make(chan error, 1)}))

	go func() { _ = o.Run(t.Context()) }()
	t.Cleanup(func() { assert.NoError(t, o.Stop()) })

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost,
		fmt.Sprintf("http://127.0.0.1:%d/v1/logs", o.Spec().HTTPPort), bytes.NewReader(logsBody(t)))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/x-protobuf")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err, "the connection was cut")
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestNewDeliveryTimeout(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		timeout   string
		expected  time.Duration
		expectErr bool
	}{
		{name: "default", timeout: "", expected: 8 * time.Second},
		{name: "valid", timeout: "30s", expected: 30 * time.Second},
		{name: "invalid", timeout: "soon", expectErr: true},
		{name: "zero", timeout: "0s", expectErr: true},
		{name: "negative", timeout: "-1s", expectErr: true},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			spec := map[string]any{}
			if c.timeout != "" {
				spec["delivery_timeout"] = c.timeout
			}
			o, err := newOTLP(t, spec)
			if c.expectErr {
				assert.ErrorContains(t, err, "invalid delivery_timeout")
				return
			}
			require.NoError(t, err)
			assert.Equal(t, c.expected, o.DeliveryTimeout())
			assert.NoError(t, o.Stop())
		})
	}
}

func TestNewRetryAfter(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		retryAfter string
		expected   string
		expectErr  bool
	}{
		{name: "default", retryAfter: "", expected: "5"},
		{name: "whole seconds", retryAfter: "30s", expected: "30"},
		{name: "fraction is rounded up", retryAfter: "1500ms", expected: "2"},
		{name: "below a second is rounded up", retryAfter: "100ms", expected: "1"},
		{name: "invalid", retryAfter: "soon", expectErr: true},
		{name: "zero", retryAfter: "0s", expectErr: true},
		{name: "negative", retryAfter: "-1s", expectErr: true},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			spec := map[string]any{}
			if c.retryAfter != "" {
				spec["retry_after"] = c.retryAfter
			}
			o, err := newOTLP(t, spec)
			if c.expectErr {
				assert.ErrorContains(t, err, "invalid retry_after")
				return
			}
			require.NoError(t, err)
			assert.Equal(t, c.expected, otlp.RetryAfter(o))
			assert.NoError(t, o.Stop())
		})
	}
}
