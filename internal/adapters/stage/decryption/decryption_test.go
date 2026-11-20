// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package decryption_test

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
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

	"github.com/go-tram/tram/internal/adapters/crypto"
	"github.com/go-tram/tram/internal/adapters/stage/decryption"
	"github.com/go-tram/tram/internal/adapters/stage/encryption"
	"github.com/go-tram/tram/internal/core/domain"
)

// key and otherKey are base64-encoded 32-byte test keys:
// "0123456789abcdef0123456789abcdef" and "fedcba9876543210fedcba9876543210".
const (
	key      = "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY="
	otherKey = "ZmVkY2JhOTg3NjU0MzIxMGZlZGNiYTk4NzY1NDMyMTA="
)

type capture struct{ events []*domain.Event }

func (c *capture) Name() string { return "capture" }
func (c *capture) From() string { return "decryption" }
func (c *capture) Stop() error  { return nil }
func (c *capture) Notify(_ context.Context, e *domain.Event) error {
	c.events = append(c.events, e)
	return nil
}

func localSpec(key string, attributes ...string) map[string]any {
	spec := map[string]any{
		"driver": "local",
		"local":  map[string]any{"key": key},
	}
	if attributes != nil {
		spec["attributes"] = attributes
	}
	return spec
}

func newDecryption(t *testing.T, spec map[string]any) (*decryption.Decryption, *capture) {
	t.Helper()
	s, err := decryption.New(domain.StageSpec{Name: "dec", From: "enc", Spec: spec}, validator.New(), slog.Default())
	require.NoError(t, err)
	c := &capture{}
	require.NoError(t, s.Subscribe(c))
	return s, c
}

func newEncryption(t *testing.T, spec map[string]any) (*encryption.Encryption, *capture) {
	t.Helper()
	s, err := encryption.New(domain.StageSpec{Name: "enc", From: "source", Spec: spec}, validator.New(), slog.Default())
	require.NoError(t, err)
	c := &capture{}
	require.NoError(t, s.Subscribe(c))
	return s, c
}

// localKeyID computes the key ID the local driver derives from a base64 key.
func localKeyID(key string) string {
	raw, err := base64.StdEncoding.DecodeString(key)
	if err != nil {
		panic(err)
	}
	sum := sha256.Sum256(raw)
	return "local:sha256:" + hex.EncodeToString(sum[:8])
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

func TestRoundtripThroughEncryptionStage(t *testing.T) {
	t.Parallel()
	enc, encOut := newEncryption(t, localSpec(key, "user.email"))
	dec, decOut := newDecryption(t, localSpec(key, "user.email"))

	in := event(&logspb.LogRecord{
		Body:       &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "user logged in"}},
		Attributes: []*commonpb.KeyValue{stringAttr("team", "team-0"), stringAttr("user.email", "a@b.c")},
	})
	original := proto.Clone(in.Logs).(*logspb.LogsData)

	require.NoError(t, enc.Notify(context.Background(), in))
	require.Len(t, encOut.events, 1)
	encrypted := firstRecord(encOut.events[0])
	assert.NotEqual(t, "user logged in", encrypted.GetBody().GetStringValue())
	assert.True(t, proto.Equal(original, in.Logs), "the encryption stage must not modify its input")

	require.NoError(t, dec.Notify(context.Background(), encOut.events[0]))
	require.Len(t, decOut.events, 1)
	out := firstRecord(decOut.events[0])

	assert.Equal(t, "user logged in", out.GetBody().GetStringValue())
	email, ok := attrOf(out, "user.email")
	require.True(t, ok)
	assert.Equal(t, "a@b.c", email)
	team, ok := attrOf(out, "team")
	require.True(t, ok)
	assert.Equal(t, "team-0", team)
	for _, marker := range []string{decryption.AttrAlgorithm, decryption.AttrKeyID, decryption.AttrWrappedKey} {
		_, found := attrOf(out, marker)
		assert.False(t, found, "marker attribute %s must be removed", marker)
	}
}

func TestUnmarkedRecordPassesThrough(t *testing.T) {
	t.Parallel()
	dec, decOut := newDecryption(t, localSpec(key, "user.email"))
	in := event(&logspb.LogRecord{
		Body:       &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "plain"}},
		Attributes: []*commonpb.KeyValue{stringAttr("team", "team-0")},
	})

	require.NoError(t, dec.Notify(context.Background(), in))
	require.Len(t, decOut.events, 1)
	out := firstRecord(decOut.events[0])
	assert.Equal(t, "plain", out.GetBody().GetStringValue())
	assert.True(t, proto.Equal(in.Logs, decOut.events[0].Logs), "an unmarked event is forwarded unchanged")
}

func TestWrongKeyRejectsEvent(t *testing.T) {
	t.Parallel()
	enc, encOut := newEncryption(t, localSpec(key))
	dec, decOut := newDecryption(t, localSpec(otherKey))

	require.NoError(t, enc.Notify(context.Background(), event(&logspb.LogRecord{
		Body: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "secret"}},
	})))
	require.Len(t, encOut.events, 1)

	err := dec.Notify(context.Background(), encOut.events[0])
	require.ErrorIs(t, err, domain.ErrEventRejected)
	require.ErrorIs(t, err, decryption.ErrKeyMismatch)
	assert.Empty(t, decOut.events)
}

func TestAlgorithmMismatchRejectsEvent(t *testing.T) {
	t.Parallel()
	dec, _ := newDecryption(t, localSpec(key))
	in := event(&logspb.LogRecord{
		Body: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "x"}},
		Attributes: []*commonpb.KeyValue{
			stringAttr(decryption.AttrAlgorithm, "aes-128-gcm"),
			stringAttr(decryption.AttrKeyID, localKeyID(key)),
		},
	})

	err := dec.Notify(context.Background(), in)
	require.ErrorIs(t, err, domain.ErrEventRejected)
	require.ErrorIs(t, err, decryption.ErrAlgorithmMismatch)
}

func TestNonStringListedAttributeRejectsEvent(t *testing.T) {
	t.Parallel()
	dec, _ := newDecryption(t, localSpec(key, "retries"))
	in := event(&logspb.LogRecord{
		Attributes: []*commonpb.KeyValue{
			intAttr("retries", 3),
			stringAttr(decryption.AttrAlgorithm, "aes-256-gcm"),
			stringAttr(decryption.AttrKeyID, localKeyID(key)),
		},
	})

	err := dec.Notify(context.Background(), in)
	require.ErrorIs(t, err, domain.ErrEventRejected)
	require.ErrorIs(t, err, decryption.ErrNonStringAttribute)
}

func TestMetricsPassThroughUnchanged(t *testing.T) {
	t.Parallel()
	dec, decOut := newDecryption(t, localSpec(key))
	metrics := &metricspb.MetricsData{}
	in := &domain.Event{Metrics: metrics}

	require.NoError(t, dec.Notify(context.Background(), in))
	require.Len(t, decOut.events, 1)
	assert.Same(t, metrics, decOut.events[0].Metrics)
}

func TestCustomMarkerPrefix(t *testing.T) {
	t.Parallel()
	encSpec := localSpec(key, "user.email")
	encSpec["marker_prefix"] = "acme.crypto"
	decSpec := localSpec(key, "user.email")
	decSpec["marker_prefix"] = "acme.crypto"
	enc, encOut := newEncryption(t, encSpec)
	dec, decOut := newDecryption(t, decSpec)

	in := event(&logspb.LogRecord{
		Body:       &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "secret"}},
		Attributes: []*commonpb.KeyValue{stringAttr("user.email", "a@b.c")},
	})
	require.NoError(t, enc.Notify(context.Background(), in))
	encRecord := firstRecord(encOut.events[0])
	for _, marker := range []string{"acme.crypto.algorithm", "acme.crypto.key_id"} {
		_, ok := attrOf(encRecord, marker)
		assert.True(t, ok, "marker %s must be set", marker)
	}
	_, ok := attrOf(encRecord, decryption.AttrAlgorithm)
	assert.False(t, ok, "the default marker must not be set")

	require.NoError(t, dec.Notify(context.Background(), encOut.events[0]))
	out := firstRecord(decOut.events[0])
	assert.Equal(t, "secret", out.GetBody().GetStringValue())
	email, ok := attrOf(out, "user.email")
	require.True(t, ok)
	assert.Equal(t, "a@b.c", email)
	for _, marker := range []string{"acme.crypto.algorithm", "acme.crypto.key_id", "acme.crypto.wrapped_key"} {
		_, found := attrOf(out, marker)
		assert.False(t, found, "marker %s must be removed", marker)
	}
}

func TestNewRejectsInvalidMarkerPrefix(t *testing.T) {
	t.Parallel()
	spec := localSpec(key)
	spec["marker_prefix"] = "acme."
	_, err := decryption.New(domain.StageSpec{Name: "dec", From: "enc", Spec: spec}, validator.New(), slog.Default())
	require.ErrorIs(t, err, crypto.ErrInvalidMarkerPrefix)
	_, err = encryption.New(domain.StageSpec{Name: "enc", From: "source", Spec: spec}, validator.New(), slog.Default())
	require.ErrorIs(t, err, crypto.ErrInvalidMarkerPrefix)
}

func TestNewRejectsBadDriver(t *testing.T) {
	t.Parallel()
	_, err := decryption.New(domain.StageSpec{Name: "dec", From: "enc", Spec: map[string]any{
		"driver": "nope", "nope": map[string]any{},
	}}, validator.New(), slog.Default())
	require.Error(t, err)
}
