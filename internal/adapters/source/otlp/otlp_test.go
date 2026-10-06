package otlp_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/hex"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"codeberg.org/eljoth/tram/internal/adapters/source/otlp"

	"github.com/go-playground/validator/v10"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	"google.golang.org/protobuf/proto"

	"codeberg.org/eljoth/tram/internal/adapters/source/base"
	"codeberg.org/eljoth/tram/internal/core/domain"
)

type recorder struct {
	events []*domain.Event
	err    error
}

func (r *recorder) Name() string { return "recorder" }
func (r *recorder) From() string { return "otlp" }
func (r *recorder) Stop() error  { return nil }
func (r *recorder) Notify(_ context.Context, e *domain.Event) error {
	r.events = append(r.events, e)
	return r.err
}

func newSource(t *testing.T, subscriberErr error) (*otlp.OTLP, *recorder) {
	t.Helper()

	src, err := base.New[otlp.Spec]("otlp", map[string]any{"http_port": 1}, validator.New())
	require.NoError(t, err)
	rec := &recorder{err: subscriberErr}
	require.NoError(t, src.Subscribe(rec))
	return otlp.NewWithoutListener(src, otlp.DefaultDeliveryTimeout), rec
}

func gzipped(t *testing.T, data []byte) []byte {
	t.Helper()

	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	_, err := gz.Write(data)
	require.NoError(t, err)
	require.NoError(t, gz.Close())
	return buf.Bytes()
}

func TestHandleLogs(t *testing.T) {
	t.Parallel()

	valid, err := proto.Marshal(&logspb.LogsData{ResourceLogs: []*logspb.ResourceLogs{{}}})
	require.NoError(t, err)

	cases := []struct {
		name            string
		body            []byte
		contentType     string
		contentEncoding string
		subscriberErr   error
		expectStatus    int
		expectEvents    int
	}{
		{name: "valid", body: valid, expectStatus: http.StatusOK, expectEvents: 1},
		{
			name:         "protobuf content type",
			body:         valid,
			contentType:  "application/x-protobuf",
			expectStatus: http.StatusOK,
			expectEvents: 1,
		},
		{
			name:         "protobuf content type alias",
			body:         valid,
			contentType:  "application/protobuf",
			expectStatus: http.StatusOK,
			expectEvents: 1,
		},
		{
			name:         "unsupported content type",
			body:         valid,
			contentType:  "text/plain",
			expectStatus: http.StatusUnsupportedMediaType,
		},
		{name: "malformed", body: []byte{0xff, 0xff, 0xff}, expectStatus: http.StatusBadRequest},
		{
			name:         "malformed JSON",
			body:         []byte(`{"resourceLogs": [`),
			contentType:  "application/json",
			expectStatus: http.StatusBadRequest,
		},
		{
			name:         "JSON trace ID that is not hex",
			body:         []byte(`{"resourceLogs":[{"scopeLogs":[{"logRecords":[{"traceId":"not hex"}]}]}]}`),
			contentType:  "application/json",
			expectStatus: http.StatusBadRequest,
		},
		{name: "too large", body: make([]byte, otlp.MaxBodyBytes+1), expectStatus: http.StatusRequestEntityTooLarge},
		{
			name:            "gzip",
			body:            gzipped(t, valid),
			contentEncoding: "gzip",
			expectStatus:    http.StatusOK,
			expectEvents:    1,
		},
		{
			// Small when compressed, too large when decompressed.
			name:            "gzip too large when decompressed",
			body:            gzipped(t, make([]byte, otlp.MaxBodyBytes+1)),
			contentEncoding: "gzip",
			expectStatus:    http.StatusRequestEntityTooLarge,
		},
		{
			name:            "malformed gzip",
			body:            valid,
			contentEncoding: "gzip",
			expectStatus:    http.StatusBadRequest,
		},
		{
			name:            "unsupported content encoding",
			body:            valid,
			contentEncoding: "br",
			expectStatus:    http.StatusUnsupportedMediaType,
		},
		{
			name:          "rejected",
			body:          valid,
			subscriberErr: domain.ErrEventRejected,
			expectStatus:  http.StatusBadRequest,
			expectEvents:  1,
		},
		{
			name:          "delivery failed",
			body:          valid,
			subscriberErr: errors.New("sink unavailable"),
			expectStatus:  http.StatusServiceUnavailable,
			expectEvents:  1,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			o, rec := newSource(t, c.subscriberErr)

			r := httptest.NewRequest(http.MethodPost, "/v1/logs", bytes.NewReader(c.body))
			if c.contentType != "" {
				r.Header.Set("Content-Type", c.contentType)
			}
			if c.contentEncoding != "" {
				r.Header.Set("Content-Encoding", c.contentEncoding)
			}
			w := httptest.NewRecorder()
			otlp.HandleLogs(o, w, r)

			assert.Equal(t, c.expectStatus, w.Code)
			assert.Len(t, rec.events, c.expectEvents)
		})
	}
}

func TestHandleLogsProtobufResponse(t *testing.T) {
	t.Parallel()

	o, _ := newSource(t, nil)
	body, err := proto.Marshal(&logspb.LogsData{ResourceLogs: []*logspb.ResourceLogs{{}}})
	require.NoError(t, err)

	r := httptest.NewRequest(http.MethodPost, "/v1/logs", bytes.NewReader(body))
	r.Header.Set("Content-Type", "application/x-protobuf")
	w := httptest.NewRecorder()
	otlp.HandleLogs(o, w, r)

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "application/x-protobuf", w.Header().Get("Content-Type"))
	assert.Empty(t, w.Body.Bytes(), "an empty response message encodes to no bytes")
}

func TestHandleLogsJSON(t *testing.T) {
	t.Parallel()

	const (
		traceID = "5B8EFFF798038103D269B633813FC60C"
		spanID  = "eee19b7ec3c1b174"
	)
	body := `{"resourceLogs":[{"scopeLogs":[{"logRecords":[{
		"traceId": "` + traceID + `",
		"spanId": "` + spanID + `",
		"severityNumber": 9,
		"timeUnixNano": "1700000000000000001",
		"body": {"stringValue": "hello"},
		"unknownField": true
	}]}]}]}`

	o, rec := newSource(t, nil)
	r := httptest.NewRequest(http.MethodPost, "/v1/logs", bytes.NewReader([]byte(body)))
	r.Header.Set("Content-Type", "application/json; charset=utf-8")
	w := httptest.NewRecorder()
	otlp.HandleLogs(o, w, r)

	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	assert.Equal(t, "application/json", w.Header().Get("Content-Type"))
	assert.JSONEq(t, "{}", w.Body.String())

	require.Len(t, rec.events, 1)
	record := rec.events[0].Logs.GetResourceLogs()[0].GetScopeLogs()[0].GetLogRecords()[0]
	expectTraceID, err := hex.DecodeString(traceID)
	require.NoError(t, err)
	expectSpanID, err := hex.DecodeString(spanID)
	require.NoError(t, err)
	assert.Equal(t, expectTraceID, record.GetTraceId())
	assert.Equal(t, expectSpanID, record.GetSpanId())
	assert.Equal(t, logspb.SeverityNumber_SEVERITY_NUMBER_INFO, record.GetSeverityNumber())
	assert.Equal(t, uint64(1700000000000000001), record.GetTimeUnixNano())
	assert.Equal(t, "hello", record.GetBody().GetStringValue())
}
