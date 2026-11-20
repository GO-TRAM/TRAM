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

// Markers are the names of the attributes that mark an encrypted record.
type Markers struct {
	Algorithm  string
	KeyID      string
	WrappedKey string
}

// MarkerAttributes returns the names of the marker attributes for prefix:
// "<prefix>.algorithm", "<prefix>.key_id" and "<prefix>.wrapped_key". An
// empty prefix means the [DefaultMarkerPrefix]. A prefix must not end with a
// dot or contain whitespace, so the attribute names stay valid.
func MarkerAttributes(prefix string) (Markers, error) {
	if prefix == "" {
		prefix = DefaultMarkerPrefix
	}
	if strings.HasSuffix(prefix, ".") || strings.ContainsAny(prefix, " \t\n") {
		return Markers{}, fmt.Errorf("%w: %q", ErrInvalidMarkerPrefix, prefix)
	}
	return Markers{
		Algorithm:  prefix + ".algorithm",
		KeyID:      prefix + ".key_id",
		WrappedKey: prefix + ".wrapped_key",
	}, nil
}
