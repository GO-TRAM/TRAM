// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package nats

// PublisherSpec are the options of a broker sink with driver nats.
type PublisherSpec struct {
	URL string `json:"url" validate:"required"`
}

// SubscriberSpec are the options of a broker source with driver nats. The
// stream has to exist; TRAM creates or updates only the consumer.
type SubscriberSpec struct {
	URL string `json:"url" validate:"required"`
	// Subject is the consumer's filter; wildcards are allowed.
	Subject string `json:"subject" validate:"required"`
	// Stream holds Subject. Empty means the stream is looked up by Subject
	// when the source starts.
	Stream string `json:"stream,omitempty"`
	// Durable names a durable consumer. Without it, a temporary consumer is
	// created whenever the source starts.
	Durable string `json:"durable,omitempty"`
}
