// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

// Package local is the crypto driver that encrypts with a raw AES-256 key
// the spec provides directly, through an environment variable or a file.
package local

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"os"

	"github.com/go-playground/validator/v10"

	"codeberg.org/tram/tram/internal/core/domain"
	"codeberg.org/tram/tram/internal/ports"
)

// Name is the driver's name in a component spec's `driver` field.
const Name = "local"

// keySize is the size of an AES-256 key in bytes.
const keySize = 32

var (
	// ErrInvalidKey is returned when the key is not exactly one of key,
	// key_env and key_file, or does not decode to a 32-byte AES-256 key.
	ErrInvalidKey = errors.New("invalid key")
	// ErrDecryptionFailed is returned when the ciphertext does not open with
	// the key, e.g. because the key is wrong or the data is corrupted.
	ErrDecryptionFailed = errors.New("decryption failed")
)

// Spec holds the local driver's options, under `local:` in a component spec.
// Exactly one of Key, KeyEnv and KeyFile has to be set. In a Kubernetes
// Stream resource, Key can be a secretKeyRef, which the operator resolves.
type Spec struct {
	// Key is the base64-encoded 32-byte key.
	Key string `json:"key,omitempty"`
	// KeyEnv names the environment variable that holds the base64-encoded key.
	KeyEnv string `json:"key_env,omitempty"`
	// KeyFile points to a file that holds the base64-encoded key.
	KeyFile string `json:"key_file,omitempty"`
}

// Driver creates crypters and decrypters with a local key.
type Driver struct{}

var _ interface {
	NewCrypter(map[string]any, *validator.Validate) (ports.Crypter, error)
	NewDecrypter(map[string]any, *validator.Validate) (ports.Decrypter, error)
} = Driver{}

// New returns the local driver.
func New() Driver { return Driver{} }

// NewCrypter creates a crypter from the driver's options.
func (d Driver) NewCrypter(opts map[string]any, validator *validator.Validate) (ports.Crypter, error) {
	c, err := d.new(opts, validator)
	if err != nil {
		return nil, err
	}
	return c, nil
}

// NewDecrypter creates a decrypter from the driver's options.
func (d Driver) NewDecrypter(opts map[string]any, validator *validator.Validate) (ports.Decrypter, error) {
	return d.new(opts, validator)
}

func (Driver) new(opts map[string]any, validator *validator.Validate) (*crypter, error) {
	spec, err := domain.ParseSpec(opts, new(Spec))
	if err != nil {
		return nil, err
	}
	if err = validator.Struct(spec); err != nil {
		return nil, err
	}
	key, err := readKey(spec)
	if err != nil {
		return nil, err
	}
	return newCrypter(key)
}

// readKey returns the key exactly one of the spec's fields provides.
func readKey(spec *Spec) ([]byte, error) {
	sources := []string{spec.Key, spec.KeyEnv, spec.KeyFile}
	set := 0
	for _, s := range sources {
		if s != "" {
			set++
		}
	}
	if set != 1 {
		return nil, fmt.Errorf("%w: exactly one of key, key_env and key_file has to be set", ErrInvalidKey)
	}

	encoded := spec.Key
	if spec.KeyEnv != "" {
		encoded = os.Getenv(spec.KeyEnv)
		if encoded == "" {
			return nil, fmt.Errorf("%w: environment variable %s is empty", ErrInvalidKey, spec.KeyEnv)
		}
	}
	if spec.KeyFile != "" {
		data, err := os.ReadFile(spec.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("%w: failed to read key file: %w", ErrInvalidKey, err)
		}
		encoded = string(data)
	}

	key, err := base64.StdEncoding.DecodeString(trimSpace(encoded))
	if err != nil || len(key) != keySize {
		return nil, fmt.Errorf("%w: the key must be base64-encoded and %d bytes long", ErrInvalidKey, keySize)
	}
	return key, nil
}

func trimSpace(s string) string {
	for len(s) > 0 && (s[len(s)-1] == '\n' || s[len(s)-1] == '\r' || s[len(s)-1] == ' ' || s[len(s)-1] == '\t') {
		s = s[:len(s)-1]
	}
	return s
}

// crypter encrypts with AES-256-GCM directly under the key.
type crypter struct {
	gcm   cipher.AEAD
	keyID string
}

func newCrypter(key []byte) (*crypter, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidKey, err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidKey, err)
	}
	sum := sha256.Sum256(key)
	return &crypter{gcm: gcm, keyID: "local:sha256:" + hex.EncodeToString(sum[:8])}, nil
}

// Encrypt returns nonce and AES-256-GCM ciphertext of plaintext.
func (c *crypter) Encrypt(plaintext []byte) ([]byte, error) {
	nonce := make([]byte, c.gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("failed to read nonce: %w", err)
	}
	return c.gcm.Seal(nonce, nonce, plaintext, nil), nil
}

func (c *crypter) KeyID() string      { return c.keyID }
func (c *crypter) WrappedKey() []byte { return nil }

// Decrypt opens nonce and AES-256-GCM ciphertext with the key. A local key
// decrypts directly, so wrappedKey is ignored.
func (c *crypter) Decrypt(_ context.Context, _, ciphertext []byte) ([]byte, error) {
	nonceSize := c.gcm.NonceSize()
	if len(ciphertext) < nonceSize {
		return nil, fmt.Errorf("%w: ciphertext shorter than the nonce", ErrDecryptionFailed)
	}
	plaintext, err := c.gcm.Open(nil, ciphertext[:nonceSize], ciphertext[nonceSize:], nil)
	if err != nil {
		return nil, fmt.Errorf("%w: wrong key or corrupted ciphertext", ErrDecryptionFailed)
	}
	return plaintext, nil
}
