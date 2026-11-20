// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package stackitkms_test

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-playground/validator/v10"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"codeberg.org/tram/tram/internal/adapters/crypto/stackitkms"
)

// kmsMock answers the KMS Encrypt operation with a fixed wrapped key.
func kmsMock(t *testing.T, wrapped []byte) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/v1/projects/p/regions/eu01/keyrings/kr/keys/k/versions/3/encrypt", r.URL.Path)
		assert.Equal(t, "Bearer token", r.Header.Get("Authorization"))
		var payload struct {
			Data string `json:"data"`
		}
		if !assert.NoError(t, json.NewDecoder(r.Body).Decode(&payload)) {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_, err := base64.StdEncoding.DecodeString(payload.Data)
		if !assert.NoError(t, err, "the data key must be sent base64-encoded") {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		assert.NoError(t, json.NewEncoder(w).Encode(map[string]string{
			"data": base64.StdEncoding.EncodeToString(wrapped),
		}))
	}))
	t.Cleanup(server.Close)
	return server
}

func spec(endpoint string) map[string]any {
	return map[string]any{
		"project_id":  "p",
		"region":      "eu01",
		"keyring_id":  "kr",
		"key_id":      "k",
		"key_version": 3,
		"endpoint":    endpoint,
		"token":       "token",
	}
}

func TestWrapDataKeyAndEncrypt(t *testing.T) {
	t.Parallel()
	wrapped := []byte("kms-wrapped-data-key")
	server := kmsMock(t, wrapped)

	c, err := stackitkms.New().NewCrypter(spec(server.URL), validator.New())
	require.NoError(t, err)
	assert.Equal(t, wrapped, c.WrappedKey())
	assert.Equal(t, "stackit-kms:p/eu01/kr/k:3", c.KeyID())

	ciphertext, err := c.Encrypt([]byte("hello"))
	require.NoError(t, err)
	assert.NotEqual(t, []byte("hello"), ciphertext)
	assert.GreaterOrEqual(t, len(ciphertext), 12+5+16, "nonce, plaintext and GCM tag")
}

func TestMissingOptions(t *testing.T) {
	t.Parallel()
	_, err := stackitkms.New().NewCrypter(map[string]any{"project_id": "p"}, validator.New())
	require.Error(t, err)
	assert.NotContains(t, err.Error(), stackitkms.ErrKMS.Error())
}

func TestKMSFailure(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(server.Close)
	_, err := stackitkms.New().NewCrypter(spec(server.URL), validator.New())
	require.ErrorIs(t, err, stackitkms.ErrKMS)
	assert.Contains(t, err.Error(), "wrap")
}
