// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package amqp

// ConnectionSpec are the options both a publisher and a subscriber connect
// with.
type ConnectionSpec struct {
	// URL of the broker, amqp://host:5672 or amqps://host:5671.
	URL string `json:"url" validate:"required"`
	// Username and Password authenticate with SASL PLAIN; without them the
	// connection uses SASL ANONYMOUS. In a Stream resource, take them from a
	// secret with valueFrom.secretKeyRef.
	Username string `json:"username,omitempty" validate:"required_with=Password"`
	Password string `json:"password,omitempty" validate:"required_with=Username"`
	// InsecureSkipVerify skips the verification of the broker's certificate
	// with amqps, for tests only.
	InsecureSkipVerify bool `json:"insecure_skip_verify,omitempty"`
}

// PublisherSpec are the options of a broker sink with driver amqp.
type PublisherSpec struct {
	ConnectionSpec

	// MaxMessageBytes is the largest message the sink sends; larger ones
	// reject the event. Zero means only the limit the broker announces for
	// each address.
	MaxMessageBytes int `json:"max_message_bytes,omitempty" validate:"omitempty,min=1"`
}

// SubscriberSpec are the options of a broker source with driver amqp. The
// address has to exist, unless the broker creates addresses on demand.
type SubscriberSpec struct {
	ConnectionSpec

	// Address is read from, e.g. a queue. Its form depends on the broker,
	// e.g. a queue name on ActiveMQ Artemis, /queues/<name> on RabbitMQ 4 or
	// a queue name on Solace.
	Address string `json:"address" validate:"required"`
}
