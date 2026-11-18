// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package kafka

// ConnectionSpec are the options both a publisher and a subscriber connect
// with.
type ConnectionSpec struct {
	// Brokers are the seed brokers, host:port.
	Brokers []string `json:"brokers" validate:"required,min=1,dive,required"`
	// TLS connects with TLS; nil means plain TCP.
	TLS *TLSSpec `json:"tls,omitempty"`
	// SASL authenticates; nil means no authentication.
	SASL *SASLSpec `json:"sasl,omitempty"`
}

// TLSSpec configures TLS with the system's certificate authorities.
type TLSSpec struct {
	// InsecureSkipVerify skips the verification of the broker's certificate,
	// for tests only.
	InsecureSkipVerify bool `json:"insecure_skip_verify,omitempty"`
}

// SASLSpec authenticates with a username and password. In a Stream resource,
// take them from a secret with valueFrom.secretKeyRef.
type SASLSpec struct {
	Mechanism string `json:"mechanism" validate:"required,oneof=plain scram-sha-256 scram-sha-512"`
	Username  string `json:"username"  validate:"required"`
	Password  string `json:"password"  validate:"required"`
}

// PublisherSpec are the options of a broker sink with driver kafka.
type PublisherSpec struct {
	ConnectionSpec

	// MaxMessageBytes is the largest message the sink sends; larger ones
	// reject the event. It must not exceed the broker's message.max.bytes; at most 1 GiB.
	// Zero means defaultMaxMessageBytes.
	MaxMessageBytes int `json:"max_message_bytes,omitempty" validate:"omitempty,min=1,max=1073741824"`
}

// SubscriberSpec are the options of a broker source with driver kafka. The
// topic has to exist; TRAM joins the consumer group.
type SubscriberSpec struct {
	ConnectionSpec

	// Topic is read from all its partitions.
	Topic string `json:"topic" validate:"required"`
	// Group is the consumer group. Sources sharing it split the partitions;
	// the group's committed offsets are where a source continues.
	Group string `json:"group" validate:"required"`
}
