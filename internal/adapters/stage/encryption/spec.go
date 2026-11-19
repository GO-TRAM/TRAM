// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package encryption

// Spec is the spec of an encryption stage. The driver's options are under the
// driver's name, e.g. `local: {key: ...}` or `stackit-kms: {project_id: ...}`,
// and are passed to the driver.
type Spec struct {
	// Driver selects the key source, e.g. local or stackit-kms.
	Driver string `json:"driver" validate:"required"`
	// Algorithm is the payload encryption algorithm. Zero means the default,
	// aes-256-gcm.
	Algorithm string `json:"algorithm,omitempty" validate:"omitempty,oneof=aes-256-gcm"`
	// Attributes lists record attributes to encrypt besides the body. Only
	// string attributes can be encrypted; a listed attribute with another
	// value type rejects the event.
	Attributes []string `json:"attributes,omitempty"`
}
