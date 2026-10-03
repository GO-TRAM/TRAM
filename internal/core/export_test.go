package core

import (
	"time"

	"go.opentelemetry.io/otel/metric"

	"codeberg.org/eljoth/tram/internal/core/domain"
)

// SetRestartDelays shortens the restart delays for tests. It must be called
// before the first stream is upserted.
func (c *Core) SetRestartDelays(minDelay, maxDelay time.Duration) {
	c.minRestartDelay = minDelay
	c.maxRestartDelay = maxDelay
}

// SetMeterProvider replaces the global meter provider for the metrics of
// this core. It must be called before the first stream is upserted.
func (c *Core) SetMeterProvider(provider metric.MeterProvider) error {
	m, err := newMetrics(provider)
	c.metrics = m
	return err
}

// Unwrap returns the subscriber that the core wrapped to measure it.
func Unwrap(subscriber domain.Subscriber) domain.Subscriber {
	if s, ok := subscriber.(*instrumentedSubscriber); ok {
		return s.Subscriber
	}
	return subscriber
}
