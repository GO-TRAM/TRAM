// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package celfilter

type Spec struct {
	Logs    string `json:"logs,omitempty"    validate:"required_without_all=Metrics Traces"`
	Metrics string `json:"metrics,omitempty" validate:"required_without_all=Logs Traces"`
	Traces  string `json:"traces,omitempty"  validate:"required_without_all=Logs Metrics"`
}
