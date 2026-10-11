// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package batch

type Spec struct {
	MaxRecords int    `json:"max_records,omitempty" validate:"omitempty,min=1"`
	Timeout    string `json:"timeout,omitempty"`
}
