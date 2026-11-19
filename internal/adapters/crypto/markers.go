// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package crypto

import (
	"errors"
	"fmt"
	"strings"
)

// DefaultMarkerPrefix is the prefix of the marker attributes the encryption
// stage sets on every encrypted log record when the spec does not name one.
const DefaultMarkerPrefix = "tram.encryption"

// ErrInvalidMarkerPrefix is returned when a marker prefix would produce
// invalid attribute names.
var ErrInvalidMarkerPrefix = errors.New("invalid marker prefix")

// MarkerAttributes returns the names of the marker attributes for prefix:
// "<prefix>.algorithm", "<prefix>.key_id" and "<prefix>.wrapped_key". An
// empty prefix means the [DefaultMarkerPrefix]. A prefix must not end with a
// dot or contain whitespace, so the attribute names stay valid.
func MarkerAttributes(prefix string) (algorithm, keyID, wrappedKey string, err error) {
	if prefix == "" {
		prefix = DefaultMarkerPrefix
	}
	if strings.HasSuffix(prefix, ".") || strings.ContainsAny(prefix, " \t\n") {
		return "", "", "", fmt.Errorf("%w: %q", ErrInvalidMarkerPrefix, prefix)
	}
	return prefix + ".algorithm", prefix + ".key_id", prefix + ".wrapped_key", nil
}
