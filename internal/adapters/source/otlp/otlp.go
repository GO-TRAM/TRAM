package otlp

import (
	"context"
	"fmt"
	"io"
	"log"
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

type OTLP struct {
	*base.Source[Spec]
	httpServer *http.Server
}

func New(source domain.SourceSpec, validator *validator.Validate) (*OTLP, error) {
	base, err := base.New[Spec](source.Name, source.Spec, validator)
	if err != nil {
		return nil, err
	}

	spec := base.Spec()

	if !isPortAvailableUnix(base.Spec().HttpPort) {
		return nil, fmt.Errorf("%w: %d", domain.ErrPortUnavailable, spec.HttpPort)
	}

	return &OTLP{
		Source: base,
	}, nil
}

func (o *OTLP) Run(_ context.Context) error {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /v1/traces", o.handleTraces)
	mux.HandleFunc("POST /v1/metrics", o.handleMetrics)
	mux.HandleFunc("POST /v1/logs", o.handleLogs)

	o.httpServer = &http.Server{
		Addr:         fmt.Sprintf(":%d", o.Spec().HttpPort),
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  15 * time.Second,
	}

	log.Printf("Starting OTLP HTTP server on port %d for source %s", o.Spec().HttpPort, o.Name())
	if err := o.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Printf("OTLP HTTP server error: %v", err)
	}
	return nil
}

func (o *OTLP) handleMetrics(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Error reading request body", http.StatusBadRequest)
		return
	}

	defer r.Body.Close()

	metrics := metricspb.MetricsData{}
	if err := proto.Unmarshal(body, &metrics); err != nil {
		http.Error(w, "Error parsing request body", http.StatusBadRequest)
	}

	if err := o.NotifySubscribers(r.Context(), &domain.Event{
		Metrics: &metrics,
	}); err != nil {
		http.Error(w, "Error notifying subscribers", http.StatusInternalServerError)
	}
}

func (o *OTLP) handleLogs(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Error reading request body", http.StatusBadRequest)
		return
	}

	defer r.Body.Close()

	logs := logspb.LogsData{}
	if err := proto.Unmarshal(body, &logs); err != nil {
		http.Error(w, "Error parsing request body", http.StatusBadRequest)
	}

	if err := o.NotifySubscribers(r.Context(), &domain.Event{
		Logs: &logs,
	}); err != nil {
		http.Error(w, "Error notifying subscribers", http.StatusInternalServerError)
	}
}

func (o *OTLP) handleTraces(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Error reading request body", http.StatusBadRequest)
		return
	}

	defer r.Body.Close()

	traces := tracepb.TracesData{}
	if err := proto.Unmarshal(body, &traces); err != nil {
		http.Error(w, "Error parsing request body", http.StatusBadRequest)
	}

	if err := o.NotifySubscribers(r.Context(), &domain.Event{
		Traces: &traces,
	}); err != nil {
		http.Error(w, "Error notifying subscribers", http.StatusInternalServerError)
	}
}

func (o *OTLP) Stop() error {
	o.httpServer.Close()
	return nil
}
