package main

import (
	"context"
	"fmt"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelprometheus "go.opentelemetry.io/otel/exporters/prometheus"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/exemplar"
	"go.opentelemetry.io/otel/sdk/resource"
)

// setupMetrics installs a global meter provider whose metrics, together with
// Go runtime and process metrics, are served in the Prometheus format by the
// returned handler. The returned function shuts the provider down.
func setupMetrics() (http.Handler, func(context.Context) error, error) {
	// A registry of its own instead of the global one, so the server can be
	// set up more than once in a process, as in tests.
	registry := prometheus.NewRegistry()
	registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	exporter, err := otelprometheus.New(otelprometheus.WithRegisterer(registry))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create Prometheus exporter: %w", err)
	}

	provider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(exporter),
		// Exemplars link measurements to traces, which Tram does not record
		// yet; collecting them would only cost time on every measurement.
		sdkmetric.WithExemplarFilter(exemplar.AlwaysOffFilter),
		sdkmetric.WithResource(resource.NewSchemaless(attribute.String("service.name", "tram"))),
	)
	otel.SetMeterProvider(provider)

	return promhttp.HandlerFor(registry, promhttp.HandlerOpts{}), provider.Shutdown, nil
}
