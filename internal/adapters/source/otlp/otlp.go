// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package otlp

import (
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-playground/validator/v10"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"

	"codeberg.org/tram/tram/internal/adapters/source/base"
	"codeberg.org/tram/tram/internal/core/domain"
)

const (
	Type = "otlp"
)

const (
	shutdownTimeout = 5 * time.Second
	maxBodyBytes    = 20 << 20

	readHeaderTimeout = 5 * time.Second
	readTimeout       = time.Minute
	idleTimeout       = 15 * time.Second

	defaultDeliveryTimeout = 8 * time.Second
	responseWriteTimeout   = 10 * time.Second

	defaultRetryAfter = 5 * time.Second
)

const (
	contentTypeProtobuf      = "application/x-protobuf"
	contentTypeProtobufAlias = "application/protobuf"
	contentTypeJSON          = "application/json"
)

var (
	errBodyTooLarge        = errors.New("request body too large")
	errUnsupportedEncoding = errors.New("unsupported content encoding")
)

type OTLP struct {
	*base.Source[Spec]

	logger     *slog.Logger
	listener   net.Listener
	httpServer *http.Server

	deliveryTimeout      time.Duration
	responseWriteTimeout time.Duration
	// retryAfter is the Retry-After header of retriable responses.
	retryAfter string
}

func New(source domain.SourceSpec, validator *validator.Validate, logger *slog.Logger) (*OTLP, error) {
	base, err := base.New[Spec](source.Name, source.Spec, validator)
	if err != nil {
		return nil, err
	}

	deliveryTimeout, err := positiveDuration("delivery_timeout", base.Spec().DeliveryTimeout, defaultDeliveryTimeout)
	if err != nil {
		return nil, err
	}
	retryAfter, err := positiveDuration("retry_after", base.Spec().RetryAfter, defaultRetryAfter)
	if err != nil {
		return nil, err
	}

	var lc net.ListenConfig
	listener, err := lc.Listen(context.Background(), "tcp", fmt.Sprintf(":%d", base.Spec().HTTPPort))
	if err != nil {
		return nil, fmt.Errorf("%w: %d: %w", domain.ErrPortUnavailable, base.Spec().HTTPPort, err)
	}

	o := &OTLP{
		Source:               base,
		logger:               logger,
		listener:             listener,
		deliveryTimeout:      deliveryTimeout,
		responseWriteTimeout: responseWriteTimeout,
		retryAfter:           retryAfterHeader(retryAfter),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/traces", o.handleTraces)
	mux.HandleFunc("POST /v1/metrics", o.handleMetrics)
	mux.HandleFunc("POST /v1/logs", o.handleLogs)

	// The server sets no WriteTimeout: it would also run while the request is
	// passed on, and cut the connection after a slow but successful delivery,
	// so the client retries data that arrived. handle bounds the delivery and
	// the response write separately instead.
	o.httpServer = &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: readHeaderTimeout,
		ReadTimeout:       readTimeout,
		IdleTimeout:       idleTimeout,
	}

	return o, nil
}

func positiveDuration(field, raw string, fallback time.Duration) (time.Duration, error) {
	if raw == "" {
		return fallback, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("invalid %s: %w", field, err)
	}
	if d <= 0 {
		return 0, fmt.Errorf("invalid %s: %s is not positive", field, raw)
	}
	return d, nil
}

// retryAfterHeader formats d as the whole seconds Retry-After holds. A delay
// below a second is rounded up, so a client is never told to retry at once.
func retryAfterHeader(d time.Duration) string {
	return strconv.FormatInt(int64((d+time.Second-1)/time.Second), 10)
}

func (o *OTLP) DeliveryTimeout() time.Duration { return o.deliveryTimeout }

func (o *OTLP) Run(ctx context.Context) error {
	o.logger.InfoContext(ctx, "Starting OTLP HTTP server",
		slog.String("source", o.Name()), slog.Int("port", o.Spec().HTTPPort))
	if err := o.httpServer.Serve(o.listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
		o.logger.ErrorContext(ctx, "OTLP HTTP server failed", slog.String("source", o.Name()), slog.Any("error", err))
	}
	return nil
}

func (o *OTLP) handleMetrics(w http.ResponseWriter, r *http.Request) {
	metrics := &metricspb.MetricsData{}
	o.handle(w, r, metrics, &domain.Event{Metrics: metrics})
}

func (o *OTLP) handleLogs(w http.ResponseWriter, r *http.Request) {
	logs := &logspb.LogsData{}
	o.handle(w, r, logs, &domain.Event{Logs: logs})
}

func (o *OTLP) handleTraces(w http.ResponseWriter, r *http.Request) {
	traces := &tracepb.TracesData{}
	o.handle(w, r, traces, &domain.Event{Traces: traces})
}

func (o *OTLP) handle(w http.ResponseWriter, r *http.Request, msg proto.Message, event *domain.Event) {
	defer r.Body.Close()

	contentType, status, message := o.receive(w, r, msg, event)

	_ = http.NewResponseController(w).SetWriteDeadline(time.Now().Add(o.responseWriteTimeout))

	if status != http.StatusOK {
		if status == http.StatusServiceUnavailable {
			// OTLP clients wait this long before retrying, instead of using
			// their own backoff.
			w.Header().Set("Retry-After", o.retryAfter)
		}
		http.Error(w, message, status)
		return
	}
	w.Header().Set("Content-Type", contentType)
	w.WriteHeader(http.StatusOK)
	if contentType == contentTypeJSON {
		_, _ = w.Write([]byte("{}"))
	}
}

func (o *OTLP) receive(
	w http.ResponseWriter,
	r *http.Request,
	msg proto.Message,
	event *domain.Event,
) (string, int, string) {
	contentType, ok := requestContentType(r)
	if !ok {
		return "", http.StatusUnsupportedMediaType, "Unsupported content type"
	}

	body, request, err := readBody(w, r)
	if err != nil {
		_, maxBytesExceeded := errors.AsType[*http.MaxBytesError](err)
		switch {
		case maxBytesExceeded || errors.Is(err, errBodyTooLarge):
			return "", http.StatusRequestEntityTooLarge, "Request body too large"
		case errors.Is(err, errUnsupportedEncoding):
			return "", http.StatusUnsupportedMediaType, "Unsupported content encoding"
		default:
			return "", http.StatusUnprocessableEntity, "Error reading request body"
		}
	}

	// Only failures the request did not cause are retriable. A request that
	// was answered with 422 would be answered the same way again.
	if contentType == contentTypeJSON {
		err = unmarshalJSON(body, msg)
	} else {
		err = proto.Unmarshal(body, msg)
	}
	if err != nil {
		return "", http.StatusUnprocessableEntity, "Error parsing request body"
	}
	if key, duplicate := duplicateKey(event); duplicate {
		return "", http.StatusUnprocessableEntity, fmt.Sprintf("Attribute key %q appears more than once", key)
	}
	event.Request = request

	ctx, cancel := context.WithTimeout(r.Context(), o.deliveryTimeout)
	defer cancel()
	if err = o.NotifySubscribers(ctx, event); err != nil {
		if errors.Is(err, domain.ErrEventRejected) {
			return "", http.StatusUnprocessableEntity, "Event rejected"
		}
		// OTLP clients retry on 503, but drop the data on 500.
		return "", http.StatusServiceUnavailable, "Error notifying subscribers"
	}
	return contentType, http.StatusOK, ""
}

func requestContentType(r *http.Request) (string, bool) {
	header := r.Header.Get("Content-Type")
	if header == "" {
		return contentTypeProtobuf, true
	}
	mediaType, _, err := mime.ParseMediaType(header)
	if err != nil {
		return "", false
	}
	switch mediaType {
	case contentTypeProtobuf, contentTypeProtobufAlias:
		return contentTypeProtobuf, true
	case contentTypeJSON:
		return contentTypeJSON, true
	default:
		return "", false
	}
}

func readBody(w http.ResponseWriter, r *http.Request) ([]byte, *domain.Request, error) {
	wire := &countingReader{r: http.MaxBytesReader(w, r.Body, maxBodyBytes)}
	var body io.Reader = wire
	request := &domain.Request{Encoding: "identity"}

	switch encoding := strings.ToLower(strings.TrimSpace(r.Header.Get("Content-Encoding"))); encoding {
	case "", "identity":
	case "gzip":
		gz, err := gzip.NewReader(body)
		if err != nil {
			return nil, nil, err
		}
		defer gz.Close()
		body = gz
		request.Encoding = encoding
	default:
		return nil, nil, errUnsupportedEncoding
	}

	data, err := io.ReadAll(io.LimitReader(body, maxBodyBytes+1))
	if err != nil {
		return nil, nil, err
	}
	if len(data) > maxBodyBytes {
		return nil, nil, errBodyTooLarge
	}
	request.WireBytes, request.DecodedBytes = wire.n, len(data)
	return data, request, nil
}

// countingReader counts the bytes read through it.
type countingReader struct {
	r io.Reader
	n int
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += n
	return n, err
}

func (o *OTLP) Stop() error {
	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	err := o.httpServer.Shutdown(ctx)
	if err != nil {
		err = errors.Join(err, o.httpServer.Close())
	}
	if cerr := o.listener.Close(); cerr != nil && !errors.Is(cerr, net.ErrClosed) {
		err = errors.Join(err, cerr)
	}
	return err
}
