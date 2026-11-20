// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package local_test

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-playground/validator/v10"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/go-tram/tram/internal/adapters/crypto/local"
)

func testKey(t *testing.T) (string, []byte) {
	t.Helper()
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}
	return base64.StdEncoding.EncodeToString(key), key
}

func newCrypter(t *testing.T, opts map[string]any) interface {
	Encrypt([]byte) ([]byte, error)
	KeyID() string
	WrappedKey() []byte
} {
	t.Helper()
	c, err := local.New().NewCrypter(opts, validator.New())
	require.NoError(t, err)
	return c
}

func TestEncryptRoundtrip(t *testing.T) {
	t.Parallel()
	encoded, key := testKey(t)
	c := newCrypter(t, map[string]any{"key": encoded})

	plaintext := []byte("hello tram")
	ciphertext, err := c.Encrypt(plaintext)
	require.NoError(t, err)
	assert.NotEqual(t, plaintext, ciphertext)
	assert.Nil(t, c.WrappedKey())
	assert.Contains(t, c.KeyID(), "local:sha256:")

	block, err := aes.NewCipher(key)
	require.NoError(t, err)
	gcm, err := cipher.NewGCM(block)
	require.NoError(t, err)
	nonceSize := gcm.NonceSize()
	opened, err := gcm.Open(nil, ciphertext[:nonceSize], ciphertext[nonceSize:], nil)
	require.NoError(t, err)
	assert.Equal(t, plaintext, opened)
}

func TestKeyFromEnv(t *testing.T) {
	encoded, _ := testKey(t)
	t.Setenv("TRAM_TEST_KEY", encoded)
	c := newCrypter(t, map[string]any{"key_env": "TRAM_TEST_KEY"})
	_, err := c.Encrypt([]byte("x"))
	require.NoError(t, err)
}

func TestKeyFromFile(t *testing.T) {
	t.Parallel()
	encoded, _ := testKey(t)
	path := filepath.Join(t.TempDir(), "key")
	require.NoError(t, os.WriteFile(path, []byte(encoded+"\n"), 0o600))
	c := newCrypter(t, map[string]any{"key_file": path})
	_, err := c.Encrypt([]byte("x"))
	require.NoError(t, err)
}

func TestExactlyOneKeySource(t *testing.T) {
	t.Parallel()
	encoded, _ := testKey(t)
	_, err := local.New().NewCrypter(map[string]any{"key": encoded, "key_env": "X"}, validator.New())
	require.ErrorIs(t, err, local.ErrInvalidKey)
	_, err = local.New().NewCrypter(map[string]any{}, validator.New())
	require.ErrorIs(t, err, local.ErrInvalidKey)
}

func TestInvalidKey(t *testing.T) {
	t.Parallel()
	_, err := local.New().
		NewCrypter(map[string]any{"key": base64.StdEncoding.EncodeToString([]byte("short"))}, validator.New())
	require.ErrorIs(t, err, local.ErrInvalidKey)
	_, err = local.New().NewCrypter(map[string]any{"key": "not-base64!!"}, validator.New())
	require.ErrorIs(t, err, local.ErrInvalidKey)
}
