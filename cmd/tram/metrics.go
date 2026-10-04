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

// setupMetrics installs the global meter provider and returns a Prometheus
// handler for its metrics plus Go runtime and process metrics.
func setupMetrics() (http.Handler, func(context.Context) error, error) {
	// Not the global registry, so tests can set up several servers.
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
		// Without traces, exemplars only cost time.
		sdkmetric.WithExemplarFilter(exemplar.AlwaysOffFilter),
		sdkmetric.WithResource(resource.NewSchemaless(attribute.String("service.name", "tram"))),
	)
	otel.SetMeterProvider(provider)

	return promhttp.HandlerFor(registry, promhttp.HandlerOpts{}), provider.Shutdown, nil
}
