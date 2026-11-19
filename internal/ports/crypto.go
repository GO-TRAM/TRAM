// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package ports

import "context"

// Crypter encrypts payloads with a key its driver holds. Implementations
// must be safe for concurrent use.
type Crypter interface {
	// Encrypt returns the ciphertext of plaintext with the nonce prepended,
	// so consumers can decrypt it standalone.
	Encrypt(plaintext []byte) ([]byte, error)
	// KeyID identifies the key and its version, e.g. for a record attribute
	// a consumer reads to choose its decryption key.
	KeyID() string
	// WrappedKey returns the key-wrapped data key a consumer has to unwrap
	// with the KMS before it can decrypt, or nil when the crypter's key
	// encrypts directly, like a local key.
	WrappedKey() []byte
}

// Decrypter decrypts payloads an encryption stage produced. Implementations
// must be safe for concurrent use.
type Decrypter interface {
	// Decrypt returns the plaintext of ciphertext (nonce prepended). For KMS
	// drivers, wrappedKey is the record's wrapped data key, which is unwrapped
	// with the KMS first; it is nil for drivers whose key decrypts directly.
	Decrypt(ctx context.Context, wrappedKey, ciphertext []byte) ([]byte, error)
	// KeyID identifies the key and its version the decrypter expects, e.g. to
	// compare against a record's encryption attribute.
	KeyID() string
}
