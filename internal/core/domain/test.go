// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package domain

// StreamTest is a test case of a stream: an input event and the expected
// outcome per sink. Input and expected events are OTLP JSON objects with one
// of the keys logs, metrics or traces. An expect value of "drop" means the
// sink must receive nothing, "reject" that the event is rejected by a stage.
type StreamTest struct {
	Name   string         `json:"name"`
	Input  map[string]any `json:"input"`
	Expect map[string]any `json:"expect"`
}
