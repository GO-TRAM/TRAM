// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

// Package drivers is the one place that knows the concrete broker drivers. The
// broker sink creates a [PublisherFactory] and the broker source a
// [SubscriberFactory] to resolve the `driver` of their spec; their tests inject
// another [broker.DriverFactory] instead.
package drivers

import (
	"fmt"
	"strings"

	"github.com/go-playground/validator/v10"

	"codeberg.org/eljoth/tram/internal/adapters/broker"
	"codeberg.org/eljoth/tram/internal/adapters/broker/amqp"
	"codeberg.org/eljoth/tram/internal/adapters/broker/kafka"
	"codeberg.org/eljoth/tram/internal/adapters/broker/nats"
)

// PublisherFactory returns the publishing side of the driver a spec names, for
// the broker sink.
type PublisherFactory struct {
	validator *validator.Validate
}

var _ broker.DriverFactory[broker.PublisherDriver] = (*PublisherFactory)(nil)

func NewPublisherFactory(validator *validator.Validate) *PublisherFactory {
	return &PublisherFactory{validator: validator}
}

// Driver returns the publishing side of the driver called name.
func (f *PublisherFactory) Driver(name string) (broker.PublisherDriver, error) {
	return driver(f.validator, name)
}

// SubscriberFactory returns the subscribing side of the driver a spec names,
// for the broker source.
type SubscriberFactory struct {
	validator *validator.Validate
}

var _ broker.DriverFactory[broker.SubscriberDriver] = (*SubscriberFactory)(nil)

func NewSubscriberFactory(validator *validator.Validate) *SubscriberFactory {
	return &SubscriberFactory{validator: validator}
}

// Driver returns the subscribing side of the driver called name.
func (f *SubscriberFactory) Driver(name string) (broker.SubscriberDriver, error) {
	return driver(f.validator, name)
}

// driver creates the driver called name. Drivers are stateless, so it creates
// one per call; a driver that holds state, such as shared connections, would be
// cached by the factories and closed by their owner.
func driver(validator *validator.Validate, name string) (broker.Driver, error) {
	switch name {
	case amqp.Name:
		return amqp.New(validator), nil
	case kafka.Name:
		return kafka.New(validator), nil
	case nats.Name:
		return nats.New(validator), nil
	default:
		return nil, fmt.Errorf("%w %q, available: %s", broker.ErrUnknownDriver, name,
			strings.Join([]string{amqp.Name, kafka.Name, nats.Name}, ", "))
	}
}
