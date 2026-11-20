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
	"sync"

	"github.com/go-playground/validator/v10"
	"github.com/stackitcloud/stackit-sdk-go/core/config"
	kms "github.com/stackitcloud/stackit-sdk-go/services/kms/v1api"

	"codeberg.org/tram/tram/internal/core/domain"
	"codeberg.org/tram/tram/internal/ports"
)

// Name is the driver's name in a component spec's `driver` field.
const Name = "stackit-kms"

// keySize is the size of the generated data key in bytes (AES-256).
const keySize = 32

var (
	// ErrKMS is returned when the KMS key cannot be used, e.g. the options
	// are incomplete or wrapping the data key fails.
	ErrKMS = errors.New("STACKIT KMS error")
	// ErrDecryptionFailed is returned when a ciphertext does not decrypt, e.g.
	// because the data is corrupted.
	ErrDecryptionFailed = errors.New("decryption failed")
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

// Driver creates crypters and decrypters backed by a STACKIT KMS key.
type Driver struct{}

var _ interface {
	NewCrypter(map[string]any, *validator.Validate) (ports.Crypter, error)
	NewDecrypter(map[string]any, *validator.Validate) (ports.Decrypter, error)
} = Driver{}

// New returns the STACKIT KMS driver.
func New() Driver { return Driver{} }

// NewCrypter generates a data key, wraps it with the KMS key and returns the
// crypter that encrypts with the data key.
func (d Driver) NewCrypter(opts map[string]any, validator *validator.Validate) (ports.Crypter, error) {
	spec, client, err := d.parse(opts, validator)
	if err != nil {
		return nil, err
	}
	return newCrypter(context.Background(), spec, client)
}

// NewDecrypter returns a decrypter that unwraps the records' wrapped data keys
// with the KMS key and decrypts with them.
func (d Driver) NewDecrypter(opts map[string]any, validator *validator.Validate) (ports.Decrypter, error) {
	spec, client, err := d.parse(opts, validator)
	if err != nil {
		return nil, err
	}
	return &decrypter{client: client, spec: spec, keyID: keyID(spec), unwrapped: map[string][]byte{}}, nil
}

func (Driver) parse(opts map[string]any, validator *validator.Validate) (*Spec, *kms.APIClient, error) {
	spec, err := domain.ParseSpec(opts, new(Spec))
	if err != nil {
		return nil, nil, err
	}
	if err = validator.Struct(spec); err != nil {
		return nil, nil, err
	}
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
		return nil, nil, fmt.Errorf("%w: failed to create KMS client: %w", ErrKMS, err)
	}
	return spec, client, nil
}

func keyID(spec *Spec) string {
	return fmt.Sprintf("stackit-kms:%s/%s/%s/%s:%d",
		spec.ProjectID, spec.Region, spec.KeyRingID, spec.KeyID, spec.KeyVersion)
}

func newCrypter(ctx context.Context, spec *Spec, client *kms.APIClient) (*crypter, error) {
	dek := make([]byte, keySize)
	if _, err := rand.Read(dek); err != nil {
		return nil, fmt.Errorf("%w: failed to generate data key: %w", ErrKMS, err)
	}
	wrapped, err := client.DefaultAPI.Encrypt(ctx, spec.ProjectID, spec.Region, spec.KeyRingID, spec.KeyID, spec.KeyVersion).
		EncryptPayload(kms.EncryptPayload{Data: base64.StdEncoding.EncodeToString(dek)}).
		Execute()
	if err != nil {
		return nil, fmt.Errorf("%w: failed to wrap the data key: %w", ErrKMS, err)
	}
	wrappedKey, err := base64.StdEncoding.DecodeString(wrapped.GetData())
	if err != nil {
		return nil, fmt.Errorf("%w: KMS returned no valid wrapped key: %w", ErrKMS, err)
	}

	gcm, err := newGCM(dek)
	if err != nil {
		return nil, err
	}
	return &crypter{
		gcm:     gcm,
		wrapped: wrappedKey,
		keyID:   keyID(spec),
	}, nil
}

func newGCM(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrKMS, err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrKMS, err)
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

// decrypter unwraps the records' wrapped data keys with the KMS and decrypts
// with them. Unwrapped data keys are cached in memory, so the KMS is called
// once per wrapped key, not per record.
type decrypter struct {
	client    *kms.APIClient
	spec      *Spec
	keyID     string
	mu        sync.Mutex
	unwrapped map[string][]byte
}

// Decrypt unwraps wrappedKey with the KMS (unless cached) and opens the
// AES-256-GCM ciphertext with the data key.
func (d *decrypter) Decrypt(ctx context.Context, wrappedKey, ciphertext []byte) ([]byte, error) {
	if len(wrappedKey) == 0 {
		return nil, fmt.Errorf("%w: record has no wrapped data key", ErrKMS)
	}
	dek, err := d.unwrap(ctx, wrappedKey)
	if err != nil {
		return nil, err
	}
	gcm, err := newGCM(dek)
	if err != nil {
		return nil, err
	}
	nonceSize := gcm.NonceSize()
	if len(ciphertext) < nonceSize {
		return nil, fmt.Errorf("%w: ciphertext shorter than the nonce", ErrDecryptionFailed)
	}
	plaintext, err := gcm.Open(nil, ciphertext[:nonceSize], ciphertext[nonceSize:], nil)
	if err != nil {
		return nil, fmt.Errorf("%w: corrupted ciphertext", ErrDecryptionFailed)
	}
	return plaintext, nil
}

func (d *decrypter) KeyID() string { return d.keyID }

// unwrap returns the data key the KMS wrapped, calling the KMS Decrypt
// operation once per wrapped key and caching the result.
func (d *decrypter) unwrap(ctx context.Context, wrappedKey []byte) ([]byte, error) {
	cacheKey := base64.StdEncoding.EncodeToString(wrappedKey)
	d.mu.Lock()
	dek, ok := d.unwrapped[cacheKey]
	d.mu.Unlock()
	if ok {
		return dek, nil
	}

	decrypted, err := d.client.DefaultAPI.Decrypt(ctx, d.spec.ProjectID, d.spec.Region, d.spec.KeyRingID, d.spec.KeyID, d.spec.KeyVersion).
		DecryptPayload(kms.DecryptPayload{Data: cacheKey}).
		Execute()
	if err != nil {
		return nil, fmt.Errorf("%w: failed to unwrap the data key: %w", ErrKMS, err)
	}
	dek, err = base64.StdEncoding.DecodeString(decrypted.GetData())
	if err != nil || len(dek) != keySize {
		return nil, fmt.Errorf("%w: KMS returned no valid data key", ErrKMS)
	}

	d.mu.Lock()
	d.unwrapped[cacheKey] = dek
	d.mu.Unlock()
	return dek, nil
}
