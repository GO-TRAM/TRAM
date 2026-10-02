package otlp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"time"

	"codeberg.org/eljoth/tram/internal/adapters/source/base"
	"codeberg.org/eljoth/tram/internal/core/domain"
	"github.com/go-playground/validator/v10"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

const (
	Type = "otlp"
)

const (
	shutdownTimeout = 5 * time.Second
	// maxBodyBytes matches the OpenTelemetry Collector's default request size limit.
	maxBodyBytes = 20 << 20
)

type OTLP struct {
	*base.Source[Spec]
	listener   net.Listener
	httpServer *http.Server
}

func New(source domain.SourceSpec, validator *validator.Validate) (*OTLP, error) {
	base, err := base.New[Spec](source.Name, source.Spec, validator)
	if err != nil {
		return nil, err
	}

	// Bind right away so a taken port fails stream creation instead of
	// surfacing later in Run.
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", base.Spec().HttpPort))
	if err != nil {
		return nil, fmt.Errorf("%w: %d: %w", domain.ErrPortUnavailable, base.Spec().HttpPort, err)
	}

	o := &OTLP{
		Source:   base,
		listener: listener,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/traces", o.handleTraces)
	mux.HandleFunc("POST /v1/metrics", o.handleMetrics)
	mux.HandleFunc("POST /v1/logs", o.handleLogs)

	o.httpServer = &http.Server{
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  15 * time.Second,
	}

	return o, nil
}

func (o *OTLP) Run(_ context.Context) error {
	log.Printf("Starting OTLP HTTP server on port %d for source %s", o.Spec().HttpPort, o.Name())
	if err := o.httpServer.Serve(o.listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Printf("OTLP HTTP server error: %v", err)
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
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			http.Error(w, "Request body too large", http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, "Error reading request body", http.StatusBadRequest)
		return
	}

	if err := proto.Unmarshal(body, msg); err != nil {
		http.Error(w, "Error parsing request body", http.StatusBadRequest)
		return
	}

	if err := o.NotifySubscribers(r.Context(), event); err != nil {
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
