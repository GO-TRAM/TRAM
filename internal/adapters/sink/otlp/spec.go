// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package otlp

type Spec struct {
	URL string `json:"url" validate:"required"`

	Headers map[string]string `json:"headers,omitempty"`

	Timeout string `json:"timeout,omitempty"`

	Insecure bool `json:"insecure,omitempty"`
}
