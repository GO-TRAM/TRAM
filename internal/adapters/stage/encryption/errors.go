// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package encryption

import "errors"

var (
	// ErrUnsupportedBody is returned when a log record's body is neither a
	// string nor bytes and cannot be encrypted.
	ErrUnsupportedBody = errors.New("body is neither string nor bytes")
	// ErrNonStringAttribute is returned when a listed attribute of a record
	// has a value that is not a string.
	ErrNonStringAttribute = errors.New("listed attribute is not a string")
	// ErrEncryptionFailed is returned when encrypting a record fails.
	ErrEncryptionFailed = errors.New("encryption failed")
)
