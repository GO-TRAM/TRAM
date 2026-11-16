// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package cebroker

import (
	"context"
	"time"

	"github.com/cloudevents/sdk-go/v2/binding"
)

// Brokers the cloudevent sink and source support.
const (
	BrokerNATS = "nats"
)

// Sender publishes CloudEvents messages to destinations of its broker.
type Sender interface {
	// CheckDestination returns an error if dest is not a valid destination,
	// e.g. a NATS subject with a wildcard.
	CheckDestination(dest string) error
	// Send publishes m to dest and returns once the broker stored it.
	Send(ctx context.Context, dest string, m binding.Message) error
	Close() error
}

// Receiver delivers the CloudEvents messages a consumer of its broker gets.
type Receiver interface {
	// Open starts consuming and blocks until ctx is done.
	Open(ctx context.Context) error
	// Receive returns the next message; it fails once ctx is done or the
	// receiver is closed.
	Receive(ctx context.Context) (binding.Message, error)
	// Ack confirms a delivered message.
	Ack(m binding.Message) error
	// Retry has the broker deliver m again after delay.
	Retry(m binding.Message, delay time.Duration) error
	// Reject drops m for good, e.g. data that is not valid OTLP.
	Reject(m binding.Message) error
	Close() error
}
