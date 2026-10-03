package otlp

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/go-playground/validator/v10"

	"codeberg.org/eljoth/tram/internal/adapters/sink/base"
	"codeberg.org/eljoth/tram/internal/core/domain"
)

const (
	Type = "otlp"

	// Default timeout for HTTP requests
	defaultTimeout = 10 * time.Second

	// Let the HTTP client follow up to 10 redirects to platform endpoints that use one.
	maxRedirects = 10
)

type OTLP struct {
	*base.Sink[Spec]

	logger *slog.Logger
	client *http.Client
}

func New(sink domain.SinkSpec, validator *validator.Validate, logger *slog.Logger) (*OTLP, error) {
	baseSink, err := base.New[Spec](sink.Name, sink.From, sink.Spec, validator)
	if err != nil {
		return nil, err
	}

	// Parse timeout
	timeout := defaultTimeout
	if baseSink.Spec().Timeout != "" {
		timeout, err = time.ParseDuration(baseSink.Spec().Timeout)
		if err != nil {
			return nil, fmt.Errorf("invalid timeout: %w", err)
		}
	}

	// Configure HTTP transport
	transport := &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 100,
		IdleConnTimeout:     90 * time.Second,
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: baseSink.Spec().Insecure,
		},
	}

	client := &http.Client{
		Transport: transport,
		Timeout:   timeout,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= maxRedirects {
				return errors.New("stopped after max redirects")
			}
			return nil
		},
	}

	return &OTLP{
		Sink:   baseSink,
		logger: logger,
		client: client,
	}, nil
}

func (o *OTLP) Notify(ctx context.Context, event *domain.Event) error {
	if event == nil {
		return nil
	}

	// Extract signal and data from event
	signal, data, err := event.MarshalOTLPData()
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	// Set up HTTP request
	endpoint := strings.TrimRight(o.Spec().URL, "/") + o.getEndpointPath(signal)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	// Set headers
	req.Header.Set("Content-Type", "application/protobuf")
	for key, value := range o.Spec().Headers {
		req.Header.Set(key, value)
	}

	// Send request
	resp, err := o.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send OTLP data: %w", err)
	}
	defer resp.Body.Close()

	// Check response status
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024)) // Read limited error body
		return fmt.Errorf("OTLP server returned status %d: %s", resp.StatusCode, string(body))
	}

	// Read and discard response body to allow connection reuse
	_, err = io.Copy(io.Discard, resp.Body)
	if err != nil {
		o.logger.WarnContext(ctx, "Failed to read response body",
			slog.String("sink", o.Name()),
			slog.String("signal", string(signal)),
			slog.Any("error", err))
	}

	o.logger.DebugContext(ctx, "OTLP sink sent data",
		slog.String("sink", o.Name()),
		slog.String("signal", string(signal)),
		slog.Int("bytes", len(data)),
		slog.Int("status", resp.StatusCode))

	return nil
}

// getEndpointPath returns the OTLP HTTP path for the given signal type
func (o *OTLP) getEndpointPath(signal domain.Signal) string {
	switch signal {
	case domain.SignalLogs:
		return "/v1/logs"
	case domain.SignalMetrics:
		return "/v1/metrics"
	case domain.SignalTraces:
		return "/v1/traces"
	default:
		return ""
	}
}

func (o *OTLP) Stop() error {
	o.client.CloseIdleConnections()
	return nil
}
