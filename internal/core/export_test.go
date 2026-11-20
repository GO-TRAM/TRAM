// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package core

import (
	"time"

	"go.opentelemetry.io/otel/metric"

	"codeberg.org/tram/tram/internal/core/domain"
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

// NewFanout returns a fanout to children without metrics.
func NewFanout(name string, children []domain.Subscriber) domain.Subscriber {
	return &fanout{name: name, children: children}
}
