// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package cloudevent

import "codeberg.org/eljoth/tram/internal/adapters/cebroker"

type Spec struct {
	// Broker selects the protocol binding the events are received with.
	Broker string `json:"broker" validate:"required,oneof=nats"`
	// Workers bounds how many events are handled at once. Zero means one.
	Workers uint `json:"workers,omitempty" validate:"max=10000"`

	NATS *cebroker.NATSSourceSpec `json:"nats,omitempty" validate:"required_if=Broker nats"`
}
