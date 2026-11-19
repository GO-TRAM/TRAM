// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

// Package stackitkms is the crypto driver for the STACKIT Key Management
// Service. It encrypts with a local data key and wraps that key once with the
// KMS key (envelope encryption), so the KMS is called once per stage, not per
// record. Consumers unwrap the wrapped data key with the KMS Decrypt operation
// and decrypt the payload locally.
package stackitkms

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"

	"github.com/go-playground/validator/v10"
	"github.com/stackitcloud/stackit-sdk-go/core/config"
	kms "github.com/stackitcloud/stackit-sdk-go/services/kms/v1api"

	"codeberg.org/eljoth/tram/internal/core/domain"
	"codeberg.org/eljoth/tram/internal/ports"
)

// Name is the driver's name in a component spec's `driver` field.
const Name = "stackit-kms"

// keySize is the size of the generated data key in bytes (AES-256).
const keySize = 32

var (
	// ErrKMS is returned when the KMS key cannot be used, e.g. the options
	// are incomplete or wrapping the data key fails.
	ErrKMS = errors.New("STACKIT KMS error")
)

// Spec holds the driver's options, under `stackit-kms:` in a component spec.
// In a Kubernetes Stream resource, Token can be a secretKeyRef, which the
// operator resolves.
type Spec struct {
	// ProjectID is the STACKIT portal project UUID the key ring is part of.
	ProjectID string `json:"project_id" validate:"required"`
	// Region is the STACKIT region the key ring is located in, e.g. eu01.
	Region string `json:"region" validate:"required"`
	// KeyRingID is the key ring UUID.
	KeyRingID string `json:"keyring_id" validate:"required"`
	// KeyID is the key UUID.
	KeyID string `json:"key_id" validate:"required"`
	// KeyVersion is the version number of the key that wraps the data key.
	KeyVersion int64 `json:"key_version" validate:"required,min=1"`
	// Endpoint overrides the KMS API base URL; the default is derived from
	// Region, https://kms.api.<region>.stackit.cloud.
	Endpoint string `json:"endpoint,omitempty"`
	// Token is a bearer token of a STACKIT service account. When empty, the
	// SDK's default authentication is used, e.g. from the
	// STACKIT_SERVICE_ACCOUNT_* environment variables.
	Token string `json:"token,omitempty"`
}

// Driver creates crypters backed by a STACKIT KMS key.
type Driver struct{}

var _ interface {
	NewCrypter(map[string]any, *validator.Validate) (ports.Crypter, error)
} = Driver{}

// New returns the STACKIT KMS driver.
func New() Driver { return Driver{} }

// NewCrypter generates a data key, wraps it with the KMS key and returns the
// crypter that encrypts with the data key.
func (Driver) NewCrypter(opts map[string]any, validator *validator.Validate) (ports.Crypter, error) {
	spec, err := domain.ParseSpec(opts, new(Spec))
	if err != nil {
		return nil, err
	}
	if err = validator.Struct(spec); err != nil {
		return nil, err
	}
	return newCrypter(context.Background(), spec)
}

func newCrypter(ctx context.Context, spec *Spec) (*crypter, error) {
	endpoint := spec.Endpoint
	if endpoint == "" {
		endpoint = fmt.Sprintf("https://kms.api.%s.stackit.cloud", spec.Region)
	}
	clientOpts := []config.ConfigurationOption{config.WithEndpoint(endpoint)}
	if spec.Token != "" {
		clientOpts = append(clientOpts, config.WithToken(spec.Token))
	}
	client, err := kms.NewAPIClient(clientOpts...)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to create KMS client: %v", ErrKMS, err)
	}

	dek := make([]byte, keySize)
	if _, err = rand.Read(dek); err != nil {
		return nil, fmt.Errorf("%w: failed to generate data key: %v", ErrKMS, err)
	}
	wrapped, err := client.DefaultAPI.Encrypt(ctx, spec.ProjectID, spec.Region, spec.KeyRingID, spec.KeyID, spec.KeyVersion).
		EncryptPayload(kms.EncryptPayload{Data: base64.StdEncoding.EncodeToString(dek)}).
		Execute()
	if err != nil {
		return nil, fmt.Errorf("%w: failed to wrap the data key: %v", ErrKMS, err)
	}
	wrappedKey, err := base64.StdEncoding.DecodeString(wrapped.GetData())
	if err != nil {
		return nil, fmt.Errorf("%w: KMS returned no valid wrapped key: %v", ErrKMS, err)
	}

	gcm, err := newGCM(dek)
	if err != nil {
		return nil, err
	}
	return &crypter{
		gcm:     gcm,
		wrapped: wrappedKey,
		keyID: fmt.Sprintf("stackit-kms:%s/%s/%s/%s:%d",
			spec.ProjectID, spec.Region, spec.KeyRingID, spec.KeyID, spec.KeyVersion),
	}, nil
}

func newGCM(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrKMS, err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrKMS, err)
	}
	return gcm, nil
}

// crypter encrypts with the data key and carries the KMS-wrapped data key.
type crypter struct {
	gcm     cipher.AEAD
	wrapped []byte
	keyID   string
}

// Encrypt returns nonce and AES-256-GCM ciphertext of plaintext under the
// data key.
func (c *crypter) Encrypt(plaintext []byte) ([]byte, error) {
	nonce := make([]byte, c.gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("failed to read nonce: %w", err)
	}
	return c.gcm.Seal(nonce, nonce, plaintext, nil), nil
}

func (c *crypter) KeyID() string      { return c.keyID }
func (c *crypter) WrappedKey() []byte { return c.wrapped }
