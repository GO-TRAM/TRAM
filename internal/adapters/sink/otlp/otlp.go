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

	defaultTimeout = 10 * time.Second

	maxRedirects = 10

	maxErrorBodyBytes = 1024
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

	timeout := defaultTimeout
	if raw := baseSink.Spec().Timeout; raw != "" {
		timeout, err = time.ParseDuration(raw)
		if err != nil {
			return nil, fmt.Errorf("invalid timeout: %w", err)
		}
		if timeout <= 0 {
			return nil, fmt.Errorf("invalid timeout: %s is not positive", raw)
		}
	}

	transport, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		return nil, errors.New("default HTTP transport is not an *http.Transport")
	}
	transport = transport.Clone()
	transport.MaxIdleConnsPerHost = transport.MaxIdleConns
	transport.TLSClientConfig = &tls.Config{
		//nolint:gosec // Opt-in through the insecure option, for endpoints with self-signed certificates.
		InsecureSkipVerify: baseSink.Spec().Insecure,
	}

	client := &http.Client{
		Transport: transport,
		Timeout:   timeout,
		CheckRedirect: func(_ *http.Request, via []*http.Request) error {
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

	payloads, err := event.MarshalOTLPPayloads()
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	for _, p := range payloads {
		if err = o.send(ctx, p.Signal, p.Data); err != nil {
			return err
		}
	}
	return nil
}

func (o *OTLP) send(ctx context.Context, signal domain.Signal, data []byte) error {
	endpoint := strings.TrimRight(o.Spec().URL, "/") + o.getEndpointPath(signal)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/x-protobuf")
	for key, value := range o.Spec().Headers {
		req.Header.Set(key, value)
	}

	resp, err := o.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send OTLP data: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBodyBytes))
		err = fmt.Errorf("OTLP server returned status %d: %s", resp.StatusCode, string(body))
		if permanent(resp.StatusCode) {
			// The same data gets the same answer again, so it is rejected
			// instead of retried, e.g. by a NATS source redelivering it.
			return fmt.Errorf("%w: %w", domain.ErrEventRejected, err)
		}
		return err
	}

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

// permanent reports whether a status refuses the data itself: a client error
// other than 408 Request Timeout and 429 Too Many Requests, which OTLP
// clients retry. Server errors may go away and are retried as well.
func permanent(status int) bool {
	return status >= 400 && status < 500 &&
		status != http.StatusRequestTimeout && status != http.StatusTooManyRequests
}

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
