// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package broker

// Spec is the spec of a broker source. The driver's options are under the
// driver's name, e.g. `nats: {url: ..., subject: ...}`, and are passed to the
// driver.
type Spec struct {
	// Driver selects the broker, e.g. nats.
	Driver string `json:"driver" validate:"required"`
	// Signal is what every message holds: one serialized LogsData,
	// MetricsData or TracesData. Messages carry no header naming it.
	Signal string `json:"signal" validate:"required,oneof=logs metrics traces"`
	// Workers bounds how many messages are handled at once. Zero means one.
	Workers uint `json:"workers,omitempty" validate:"max=10000"`

	Partition *PartitionSpec `json:"partition,omitempty"`
}

// PartitionSpec reads the partition of Key from data a broker sink spread
// over Count partitions: {partition} in the driver's options is replaced by
// the partition number.
type PartitionSpec struct {
	Count int    `json:"count" validate:"required,min=1,max=1024"`
	Key   string `json:"key"   validate:"required"`
}
