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

// SetMeterProvider must be called before the first stream is upserted.
func (c *Core) SetMeterProvider(provider metric.MeterProvider) error {
	m, err := newMetrics(provider)
	c.metrics = m
	return err
}

func Unwrap(subscriber domain.Subscriber) domain.Subscriber {
	if s, ok := subscriber.(*instrumentedSubscriber); ok {
		return s.Subscriber
	}
	return subscriber
}

// Children unwraps the fan-out among a source's or stage's subscribers.
func Children(subscribers []domain.Subscriber) []domain.Subscriber {
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
