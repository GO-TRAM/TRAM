// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package stackitkms_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/go-playground/validator/v10"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"codeberg.org/tram/tram/internal/adapters/crypto/stackitkms"
)

// echoKMS answers Encrypt and Decrypt by echoing the payload's data back, so
// the wrapped key is the data key itself and a roundtrip works. It counts the
// Decrypt calls to verify the unwrap cache.
func echoKMS(t *testing.T, decryptCalls *atomic.Int32) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload struct {
			Data string `json:"data"`
		}
		if !assert.NoError(t, json.NewDecoder(r.Body).Decode(&payload)) {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if strings.HasSuffix(r.URL.Path, "/decrypt") {
			decryptCalls.Add(1)
		}
		w.Header().Set("Content-Type", "application/json")
		assert.NoError(t, json.NewEncoder(w).Encode(map[string]string{"data": payload.Data}))
	}))
	t.Cleanup(server.Close)
	return server
}

func TestDecryptRoundtrip(t *testing.T) {
	t.Parallel()
	var decryptCalls atomic.Int32
	server := echoKMS(t, &decryptCalls)

	crypter, err := stackitkms.New().NewCrypter(spec(server.URL), validator.New())
	require.NoError(t, err)
	decrypter, err := stackitkms.New().NewDecrypter(spec(server.URL), validator.New())
	require.NoError(t, err)
	assert.Equal(t, crypter.KeyID(), decrypter.KeyID())

	plaintext := []byte("hello tram")
	ciphertext, err := crypter.Encrypt(plaintext)
	require.NoError(t, err)

	opened, err := decrypter.Decrypt(context.Background(), crypter.WrappedKey(), ciphertext)
	require.NoError(t, err)
	assert.Equal(t, plaintext, opened)

	// A second record with the same wrapped key must not call the KMS again.
	ciphertext2, err := crypter.Encrypt([]byte("again"))
	require.NoError(t, err)
	opened2, err := decrypter.Decrypt(context.Background(), crypter.WrappedKey(), ciphertext2)
	require.NoError(t, err)
	assert.Equal(t, []byte("again"), opened2)
	assert.Equal(t, int32(1), decryptCalls.Load(), "the wrapped key is unwrapped once and cached")
}

func TestDecryptWithoutWrappedKey(t *testing.T) {
	t.Parallel()
	var decryptCalls atomic.Int32
	server := echoKMS(t, &decryptCalls)
	decrypter, err := stackitkms.New().NewDecrypter(spec(server.URL), validator.New())
	require.NoError(t, err)

	_, err = decrypter.Decrypt(context.Background(), nil, []byte("x"))
	require.ErrorIs(t, err, stackitkms.ErrKMS)
}

func TestDecryptCorrupted(t *testing.T) {
	t.Parallel()
	var decryptCalls atomic.Int32
	server := echoKMS(t, &decryptCalls)
	crypter, err := stackitkms.New().NewCrypter(spec(server.URL), validator.New())
	require.NoError(t, err)
	decrypter, err := stackitkms.New().NewDecrypter(spec(server.URL), validator.New())
	require.NoError(t, err)

	ciphertext, err := crypter.Encrypt([]byte("hello"))
	require.NoError(t, err)
	ciphertext[len(ciphertext)-1] ^= 0xff
	_, err = decrypter.Decrypt(context.Background(), crypter.WrappedKey(), ciphertext)
	require.ErrorIs(t, err, stackitkms.ErrDecryptionFailed)
}
