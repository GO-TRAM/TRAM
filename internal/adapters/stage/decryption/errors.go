// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package decryption

import "errors"

var (
	// ErrUnsupportedBody is returned when an encrypted record's body is
	// neither a string nor bytes and cannot be decrypted.
	ErrUnsupportedBody = errors.New("body is neither string nor bytes")
	// ErrNonStringAttribute is returned when a listed attribute of a record
	// has a value that is not a string.
	ErrNonStringAttribute = errors.New("listed attribute is not a string")
	// ErrDecryptionFailed is returned when decrypting a record fails.
	ErrDecryptionFailed = errors.New("decryption failed")
	// ErrAlgorithmMismatch is returned when a record's encryption algorithm
	// differs from the stage's configured one.
	ErrAlgorithmMismatch = errors.New("encryption algorithm mismatch")
	// ErrKeyMismatch is returned when a record's key ID differs from the
	// decrypter's, e.g. after a key rotation.
	ErrKeyMismatch = errors.New("encryption key mismatch")
)
