package otlp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
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

	readTimeout  = 5 * time.Second
	writeTimeout = 10 * time.Second
	idleTimeout  = 15 * time.Second
)

type OTLP struct {
	*base.Source[Spec]

	logger     *slog.Logger
	listener   net.Listener
	httpServer *http.Server
}

func New(source domain.SourceSpec, validator *validator.Validate, logger *slog.Logger) (*OTLP, error) {
	base, err := base.New[Spec](source.Name, source.Spec, validator)
	if err != nil {
		return nil, err
	}

	// Bind right away so a taken port fails stream creation instead of
	// surfacing later in Run.
	var lc net.ListenConfig
	listener, err := lc.Listen(context.Background(), "tcp", fmt.Sprintf(":%d", base.Spec().HTTPPort))
	if err != nil {
		return nil, fmt.Errorf("%w: %d: %w", domain.ErrPortUnavailable, base.Spec().HTTPPort, err)
	}

	o := &OTLP{
		Source:   base,
		logger:   logger,
		listener: listener,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/traces", o.handleTraces)
	mux.HandleFunc("POST /v1/metrics", o.handleMetrics)
	mux.HandleFunc("POST /v1/logs", o.handleLogs)

	o.httpServer = &http.Server{
		Handler:      mux,
		ReadTimeout:  readTimeout,
		WriteTimeout: writeTimeout,
		IdleTimeout:  idleTimeout,
	}

	return o, nil
}

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

// handle decodes the request body into msg, which event points to, and passes
// the event on to subscribers.
func (o *OTLP) handle(w http.ResponseWriter, r *http.Request, msg proto.Message, event *domain.Event) {
	defer r.Body.Close()

	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		if _, tooLarge := errors.AsType[*http.MaxBytesError](err); tooLarge {
			http.Error(w, "Request body too large", http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, "Error reading request body", http.StatusBadRequest)
		return
	}

	if err = proto.Unmarshal(body, msg); err != nil {
		http.Error(w, "Error parsing request body", http.StatusBadRequest)
		return
	}

	if err = o.NotifySubscribers(r.Context(), event); err != nil {
		if errors.Is(err, domain.ErrEventRejected) {
			http.Error(w, "Event rejected", http.StatusBadRequest)
			return
		}
		http.Error(w, "Error notifying subscribers", http.StatusInternalServerError)
	}
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
