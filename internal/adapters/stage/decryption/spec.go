// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package decryption

// Spec is the spec of a decryption stage. The driver's options are under the
// driver's name, e.g. `local: {key: ...}` or `stackit-kms: {project_id: ...}`,
// and are passed to the driver. The driver has to be the one the encryption
// stage used, with the same key.
type Spec struct {
	// Driver selects the key source, e.g. local or stackit-kms.
	Driver string `json:"driver" validate:"required"`
	// Algorithm is the payload encryption algorithm of the records. Zero
	// means the default, aes-256-gcm.
	Algorithm string `json:"algorithm,omitempty" validate:"omitempty,oneof=aes-256-gcm"`
	// Attributes lists record attributes to decrypt besides the body, e.g.
	// the ones the encryption stage encrypted.
	Attributes []string `json:"attributes,omitempty"`
	// MarkerPrefix is the prefix of the marker attributes on encrypted
	// records: "<prefix>.algorithm", "<prefix>.key_id" and
	// "<prefix>.wrapped_key". Zero means the default, tram.encryption. It
	// has to match the prefix the records were encrypted with.
	MarkerPrefix string `json:"marker_prefix,omitempty"`
}
