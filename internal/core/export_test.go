package core

import (
	"time"

	"go.opentelemetry.io/otel/metric"

	"codeberg.org/eljoth/tram/internal/core/domain"
)

func (c *Core) SetRestartDelays(minDelay, maxDelay time.Duration) {
	c.minRestartDelay = minDelay
	c.maxRestartDelay = maxDelay
}

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
