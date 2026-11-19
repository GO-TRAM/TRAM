// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package encryption_test

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"testing"

	"github.com/go-playground/validator/v10"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/protobuf/proto"

	"log/slog"

	"codeberg.org/eljoth/tram/internal/adapters/stage/encryption"
	"codeberg.org/eljoth/tram/internal/core/domain"
)

// key is the base64-encoded 32-byte test key.
var key = base64.StdEncoding.EncodeToString([]byte("0123456789abcdef0123456789abcdef"))

func rawKey() []byte {
	k, _ := base64.StdEncoding.DecodeString(key)
	return k
}

type capture struct{ events []*domain.Event }

func (c *capture) Name() string { return "capture" }
func (c *capture) From() string { return "encryption" }
func (c *capture) Stop() error  { return nil }
func (c *capture) Notify(_ context.Context, e *domain.Event) error {
	c.events = append(c.events, e)
	return nil
}

func newStage(t *testing.T, spec map[string]any) (*encryption.Encryption, *capture) {
	t.Helper()
	s, err := encryption.New(domain.StageSpec{Name: "enc", From: "source", Spec: spec}, validator.New(), slog.Default())
	require.NoError(t, err)
	c := &capture{}
	require.NoError(t, s.Subscribe(c))
	return s, c
}

func localSpec(attributes ...string) map[string]any {
	spec := map[string]any{
		"driver": "local",
		"local":  map[string]any{"key": key},
	}
	if attributes != nil {
		spec["attributes"] = attributes
	}
	return spec
}

func stringAttr(key, value string) *commonpb.KeyValue {
	return &commonpb.KeyValue{
		Key:   key,
		Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: value}},
	}
}

func intAttr(key string, value int64) *commonpb.KeyValue {
	return &commonpb.KeyValue{
		Key:   key,
		Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: value}},
	}
}

func event(records ...*logspb.LogRecord) *domain.Event {
	return &domain.Event{Logs: &logspb.LogsData{ResourceLogs: []*logspb.ResourceLogs{{
		Resource:  &resourcepb.Resource{Attributes: []*commonpb.KeyValue{stringAttr("service.name", "shop")}},
		ScopeLogs: []*logspb.ScopeLogs{{LogRecords: records}},
	}}}}
}

func firstRecord(e *domain.Event) *logspb.LogRecord {
	return e.Logs.GetResourceLogs()[0].GetScopeLogs()[0].GetLogRecords()[0]
}

func attrOf(r *logspb.LogRecord, key string) (string, bool) {
	for _, kv := range r.GetAttributes() {
		if kv.GetKey() == key {
			return kv.GetValue().GetStringValue(), true
		}
	}
	return "", false
}

// decrypt reverses the stage's body encryption with the test key.
func decrypt(t *testing.T, encoded string) string {
	t.Helper()
	ciphertext, err := base64.StdEncoding.DecodeString(encoded)
	require.NoError(t, err)
	block, err := aes.NewCipher(rawKey())
	require.NoError(t, err)
	gcm, err := cipher.NewGCM(block)
	require.NoError(t, err)
	opened, err := gcm.Open(nil, ciphertext[:gcm.NonceSize()], ciphertext[gcm.NonceSize():], nil)
	require.NoError(t, err)
	return string(opened)
}

func TestEncryptsBodyAndListedAttributes(t *testing.T) {
	t.Parallel()
	s, c := newStage(t, localSpec("user.email"))
	in := event(&logspb.LogRecord{
		Body:       &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "user logged in"}},
		Attributes: []*commonpb.KeyValue{stringAttr("team", "team-0"), stringAttr("user.email", "a@b.c")},
	})

	require.NoError(t, s.Notify(context.Background(), in))
	require.Len(t, c.events, 1)
	out := firstRecord(c.events[0])

	// The body is encrypted, not the plaintext.
	body := out.GetBody().GetStringValue()
	assert.NotEqual(t, "user logged in", body)
	assert.Equal(t, "user logged in", decrypt(t, body))

	// The listed attribute is encrypted, the routing attribute stays clear.
	encryptedEmail, ok := attrOf(out, "user.email")
	require.True(t, ok)
	assert.Equal(t, "a@b.c", decrypt(t, encryptedEmail))
	team, ok := attrOf(out, "team")
	require.True(t, ok)
	assert.Equal(t, "team-0", team, "routing attributes must stay in cleartext")

	// The encryption marker attributes are set.
	algo, ok := attrOf(out, encryption.AttrAlgorithm)
	require.True(t, ok)
	assert.Equal(t, "aes-256-gcm", algo)
	keyID, ok := attrOf(out, encryption.AttrKeyID)
	require.True(t, ok)
	assert.Contains(t, keyID, "local:sha256:")
	_, hasWrapped := attrOf(out, encryption.AttrWrappedKey)
	assert.False(t, hasWrapped, "a local key needs no wrapped data key")
}

func TestInputStaysUntouched(t *testing.T) {
	t.Parallel()
	s, c := newStage(t, localSpec())
	in := event(&logspb.LogRecord{
		Body:       &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "secret"}},
		Attributes: []*commonpb.KeyValue{stringAttr("team", "team-0")},
	})
	original := proto.Clone(in.Logs).(*logspb.LogsData)

	require.NoError(t, s.Notify(context.Background(), in))
	assert.True(t, proto.Equal(original, in.Logs), "the shared input must not be modified")
	require.Len(t, c.events, 1)
	assert.NotEqual(t, "secret", firstRecord(c.events[0]).GetBody().GetStringValue())
}

func TestMetricsPassThroughUnchanged(t *testing.T) {
	t.Parallel()
	s, c := newStage(t, localSpec())
	metrics := &metricspb.MetricsData{}
	in := &domain.Event{Metrics: metrics}

	require.NoError(t, s.Notify(context.Background(), in))
	require.Len(t, c.events, 1)
	assert.Same(t, metrics, c.events[0].Metrics, "metrics pass through unchanged in this version")
}

func TestNonStringListedAttributeRejectsEvent(t *testing.T) {
	t.Parallel()
	s, c := newStage(t, localSpec("retries"))
	in := event(&logspb.LogRecord{Attributes: []*commonpb.KeyValue{intAttr("retries", 3)}})

	err := s.Notify(context.Background(), in)
	require.ErrorIs(t, err, domain.ErrEventRejected)
	require.ErrorIs(t, err, encryption.ErrNonStringAttribute)
	assert.Empty(t, c.events)
}

func TestUnsupportedBodyRejectsEvent(t *testing.T) {
	t.Parallel()
	s, _ := newStage(t, localSpec())
	in := event(&logspb.LogRecord{
		Body: &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: 42}},
	})

	err := s.Notify(context.Background(), in)
	require.ErrorIs(t, err, domain.ErrEventRejected)
	require.ErrorIs(t, err, encryption.ErrUnsupportedBody)
}

func TestNewRejectsBadDriver(t *testing.T) {
	t.Parallel()
	_, err := encryption.New(domain.StageSpec{Name: "enc", From: "source", Spec: map[string]any{
		"driver": "nope", "nope": map[string]any{},
	}}, validator.New(), slog.Default())
	require.Error(t, err)

	_, err = encryption.New(domain.StageSpec{Name: "enc", From: "source", Spec: map[string]any{
		"driver": "local",
	}}, validator.New(), slog.Default())
	require.ErrorContains(t, err, `under "local"`)
}
