package otlp

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"codeberg.org/eljoth/tram/internal/adapters/source/base"
	"codeberg.org/eljoth/tram/internal/core/domain"
	"github.com/go-playground/validator/v10"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	"google.golang.org/protobuf/proto"
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

func TestHandleLogs(t *testing.T) {
	valid, err := proto.Marshal(&logspb.LogsData{ResourceLogs: []*logspb.ResourceLogs{{}}})
	require.NoError(t, err)

	cases := []struct {
		name          string
		body          []byte
		subscriberErr error
		expectStatus  int
		expectEvents  int
	}{
		{name: "valid", body: valid, expectStatus: http.StatusOK, expectEvents: 1},
		{name: "malformed", body: []byte{0xff, 0xff, 0xff}, expectStatus: http.StatusBadRequest},
		{name: "too large", body: make([]byte, maxBodyBytes+1), expectStatus: http.StatusRequestEntityTooLarge},
		{name: "rejected", body: valid, subscriberErr: domain.ErrEventRejected, expectStatus: http.StatusBadRequest, expectEvents: 1},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			src, err := base.New[Spec]("otlp", map[string]any{"http_port": 1}, validator.New())
			require.NoError(t, err)
			rec := &recorder{err: c.subscriberErr}
			require.NoError(t, src.Subscribe(rec))
			o := &OTLP{Source: src}

			w := httptest.NewRecorder()
			o.handleLogs(w, httptest.NewRequest(http.MethodPost, "/v1/logs", bytes.NewReader(c.body)))

			assert.Equal(t, c.expectStatus, w.Code)
			assert.Len(t, rec.events, c.expectEvents)
		})
	}
}
