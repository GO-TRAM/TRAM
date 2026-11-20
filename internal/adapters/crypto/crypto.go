// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

// Package crypto holds the contract of the key drivers of the encryption and
// decryption stages: a component spec names a `driver` and holds the
// driver's options under that name, e.g. `local: {key: ...}`, which
// [driver.Split] takes apart.
package crypto

import (
	"github.com/go-playground/validator/v10"

	"codeberg.org/eljoth/tram/internal/ports"
)

// Driver creates a [ports.Crypter] and a [ports.Decrypter] from the driver's
// options.
type Driver interface {
	NewCrypter(opts map[string]any, validator *validator.Validate) (ports.Crypter, error)
	NewDecrypter(opts map[string]any, validator *validator.Validate) (ports.Decrypter, error)
}
