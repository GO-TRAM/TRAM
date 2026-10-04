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
	"strings"
	"time"

	"github.com/go-playground/validator/v10"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"

	"codeberg.org/eljoth/tram/internal/adapters/source/base"
	"codeberg.org/eljoth/tram/internal/core/domain"
)

const (
	Type = "otlp"
)

const (
	shutdownTimeout = 5 * time.Second
	// maxBodyBytes matches the OpenTelemetry Collector's default request size limit.
	maxBodyBytes = 20 << 20

	readHeaderTimeout = 5 * time.Second
	// readTimeout bounds reading a whole request. It is generous, because a
	// body of up to maxBodyBytes can take a while on a slow connection.
	readTimeout = time.Minute
	idleTimeout = 15 * time.Second

	// defaultDeliveryTimeout bounds passing a request on, so the client gets
	// a retryable answer before it gives up itself: OpenTelemetry SDK
	// exporters wait 10s by default.
	defaultDeliveryTimeout = 8 * time.Second
	// responseWriteTimeout bounds writing the response once the request was
	// passed on, so a client that stops reading cannot hold the connection.
	responseWriteTimeout = 10 * time.Second
)

const (
	contentTypeProtobuf = "application/x-protobuf"
	// contentTypeProtobufAlias is not part of OTLP, but some clients send it,
	// among them Tram's OTLP sink before it followed the specification.
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
}

func New(source domain.SourceSpec, validator *validator.Validate, logger *slog.Logger) (*OTLP, error) {
	base, err := base.New[Spec](source.Name, source.Spec, validator)
	if err != nil {
		return nil, err
	}

	deliveryTimeout := defaultDeliveryTimeout
	if raw := base.Spec().DeliveryTimeout; raw != "" {
		deliveryTimeout, err = time.ParseDuration(raw)
		if err != nil {
			return nil, fmt.Errorf("invalid delivery_timeout: %w", err)
		}
		if deliveryTimeout <= 0 {
			return nil, fmt.Errorf("invalid delivery_timeout: %s is not positive", raw)
		}
	}

	// Bind right away so a taken port fails stream creation instead of
	// surfacing later in Run.
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

// DeliveryTimeout returns the configured delivery_timeout.
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

// handle decodes the request body into msg, which event points to, passes
// the event on to subscribers and answers the request.
func (o *OTLP) handle(w http.ResponseWriter, r *http.Request, msg proto.Message, event *domain.Event) {
	defer r.Body.Close()

	contentType, status, message := o.receive(w, r, msg, event)

	// The deadline starts now, after the delivery, which may take a while.
	// Recorders in tests do not support it, which is fine to ignore.
	_ = http.NewResponseController(w).SetWriteDeadline(time.Now().Add(o.responseWriteTimeout))

	if status != http.StatusOK {
		http.Error(w, message, status)
		return
	}
	// An empty Export*ServiceResponse encodes to no bytes in protobuf and to
	// {} in JSON.
	w.Header().Set("Content-Type", contentType)
	w.WriteHeader(http.StatusOK)
	if contentType == contentTypeJSON {
		_, _ = w.Write([]byte("{}"))
	}
}

// receive reads and decodes a request and passes it on. It returns the
// content type of the request, and the status and message to answer with.
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

	body, err := readBody(w, r)
	if err != nil {
		_, maxBytesExceeded := errors.AsType[*http.MaxBytesError](err)
		switch {
		case maxBytesExceeded || errors.Is(err, errBodyTooLarge):
			return "", http.StatusRequestEntityTooLarge, "Request body too large"
		case errors.Is(err, errUnsupportedEncoding):
			return "", http.StatusUnsupportedMediaType, "Unsupported content encoding"
		default:
			return "", http.StatusBadRequest, "Error reading request body"
		}
	}

	if contentType == contentTypeJSON {
		err = unmarshalJSON(body, msg)
	} else {
		err = proto.Unmarshal(body, msg)
	}
	if err != nil {
		return "", http.StatusBadRequest, "Error parsing request body"
	}

	// A delivery that takes too long is cancelled, so the client gets a
	// retryable answer before it gives up, and the data it sends again was
	// most likely not delivered yet. A batch that is already being sent is
	// still awaited, since it holds data of other requests too.
	ctx, cancel := context.WithTimeout(r.Context(), o.deliveryTimeout)
	defer cancel()
	if err = o.NotifySubscribers(ctx, event); err != nil {
		if errors.Is(err, domain.ErrEventRejected) {
			return "", http.StatusBadRequest, "Event rejected"
		}
		// OTLP clients retry on 503, but drop the data on 500.
		return "", http.StatusServiceUnavailable, "Error notifying subscribers"
	}
	return contentType, http.StatusOK, ""
}

// requestContentType returns the content type of an OTLP request, normalized
// to contentTypeProtobuf or contentTypeJSON. A missing content type counts as
// protobuf.
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

// readBody reads the request body and decompresses it if needed.
// maxBodyBytes limits the decompressed body too, so a small compressed
// request cannot expand without bound.
func readBody(w http.ResponseWriter, r *http.Request) ([]byte, error) {
	var body io.Reader = http.MaxBytesReader(w, r.Body, maxBodyBytes)

	switch strings.ToLower(strings.TrimSpace(r.Header.Get("Content-Encoding"))) {
	case "", "identity":
	case "gzip":
		gz, err := gzip.NewReader(body)
		if err != nil {
			return nil, err
		}
		defer gz.Close()
		body = gz
	default:
		return nil, errUnsupportedEncoding
	}

	data, err := io.ReadAll(io.LimitReader(body, maxBodyBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maxBodyBytes {
		return nil, errBodyTooLarge
	}
	return data, nil
}

// Stop waits for in-flight requests so no event reaches subscribers after it
// returns. It is safe to call before Run.
func (o *OTLP) Stop() error {
	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	err := o.httpServer.Shutdown(ctx)
	if err != nil {
		err = errors.Join(err, o.httpServer.Close())
	}
	// Shutdown only closes listeners passed to Serve; close it in case Run never started.
	if cerr := o.listener.Close(); cerr != nil && !errors.Is(cerr, net.ErrClosed) {
		err = errors.Join(err, cerr)
	}
	return err
}
