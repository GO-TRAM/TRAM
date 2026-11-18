// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package broker

// Spec is the spec of a broker sink. The driver's options are under the
// driver's name, e.g. `nats: {url: ...}`, and are passed to the driver.
type Spec struct {
	// Driver selects the broker, e.g. nats.
	Driver string `json:"driver" validate:"required"`
	// Destination resolves per record where it is sent.
	Destination DestinationSpec `json:"destination" validate:"required"`
	// MaxPending bounds the messages sent but not yet acknowledged. Zero
	// means defaultMaxPending.
	MaxPending int `json:"max_pending,omitempty" validate:"omitempty,min=1"`

	Partition *PartitionSpec `json:"partition,omitempty"`
}

// DestinationSpec holds a CEL expression per signal; signals without one are
// not sent.
type DestinationSpec struct {
	Logs    string `json:"logs,omitempty"    validate:"required_without_all=Metrics Traces"`
	Metrics string `json:"metrics,omitempty" validate:"required_without_all=Logs Traces"`
	Traces  string `json:"traces,omitempty"  validate:"required_without_all=Logs Metrics"`
}

// PartitionSpec spreads records over Count partitions by the key the
// expression of their signal returns. The destination expressions read the
// partition as the variable `partition`.
type PartitionSpec struct {
	Count   int    `json:"count"             validate:"required,min=1,max=1024"`
	Logs    string `json:"logs,omitempty"`
	Metrics string `json:"metrics,omitempty"`
	Traces  string `json:"traces,omitempty"`
}
