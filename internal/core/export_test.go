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

// SourceChildren returns the subscribers a source delivers to through the
// fan-out the core gave it, unwrapped. subscribers are the subscribers of the
// source, which hold just that fan-out.
func SourceChildren(subscribers []domain.Subscriber) []domain.Subscriber {
	var children []domain.Subscriber
	for _, subscriber := range subscribers {
		f, ok := Unwrap(subscriber).(*fanout)
		if !ok {
			return nil
		}
		for _, child := range f.children {
			children = append(children, Unwrap(child))
		}
	}
	return children
}
