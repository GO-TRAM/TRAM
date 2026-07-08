package otlp

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"codeberg.org/eljoth/stream/internal/adapters/source/base"
	"codeberg.org/eljoth/stream/internal/core/domain"
	"github.com/go-playground/validator/v10"
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

	if !isPortAvailableUnix(base.Spec().GRPCPort) {
		return nil, fmt.Errorf("%w: %d", domain.ErrPortUnavailable, spec.GRPCPort)
	}

	return &OTLP{
		Source: base,
	}, nil
}

func (o *OTLP) Run(ctx context.Context) error {
	go o.runHTTPServer()

	for {
		select {
		case <-o.Context().Done():
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (o *OTLP) runHTTPServer() {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /v1/traces", o.handleRequest)
	mux.HandleFunc("POST /v1/metrics", o.handleRequest)
	mux.HandleFunc("POST /v1/logs", o.handleRequest)

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
}

func (o *OTLP) handleRequest(w http.ResponseWriter, r *http.Request) {
	_ = o.NotifySubscribers(r.Context(), domain.Event{})
}

func (o *OTLP) Stop() error {
	o.Source.Stop()
	o.httpServer.Close()
	return nil
}
