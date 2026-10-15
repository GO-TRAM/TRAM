// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package nats

type Spec struct {
	URL     string `json:"url"               validate:"required"`
	Stream  string `json:"stream"            validate:"required"`
	Subject string `json:"subject"           validate:"required"`
	Durable string `json:"durable,omitempty"`
	Workers uint   `json:"workers,omitempty" validate:"max=10000"`
}
