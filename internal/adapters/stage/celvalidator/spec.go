// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package celvalidator

const (
	OnMismatchDrop   = "drop"
	OnMismatchReject = "reject"
)

type Spec struct {
	Logs       string `json:"logs"`
	Metrics    string `json:"metrics"`
	Traces     string `json:"traces"`
	OnMismatch string `json:"on_mismatch" validate:"required,oneof=drop reject"`
}
