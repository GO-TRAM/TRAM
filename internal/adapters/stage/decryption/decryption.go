// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

// Package decryption is a stage that reverses the encryption stage: it
// decrypts the body of every encrypted log record and the record attributes
// its spec lists, and removes the encryption marker attributes. Records
// without a marker attribute pass through unchanged, so the stage is
// idempotent. Metrics and traces pass through unchanged.
package decryption

import (
	"context"
	"encoding/base64"
	"fmt"
	"log/slog"

	"github.com/go-playground/validator/v10"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"

	"codeberg.org/eljoth/tram/internal/adapters/crypto"
	"codeberg.org/eljoth/tram/internal/adapters/crypto/drivers"
	"codeberg.org/eljoth/tram/internal/adapters/driver"
	"codeberg.org/eljoth/tram/internal/adapters/stage/base"
	"codeberg.org/eljoth/tram/internal/core/domain"
	"codeberg.org/eljoth/tram/internal/ports"
)

// Type is the stage's type in a stream spec.
const Type = "decryption"

// DefaultAlgorithm is the payload encryption used when the spec does not name
// one.
const DefaultAlgorithm = "aes-256-gcm"

// Record attributes the encryption stage sets by default and this stage
// removes after decrypting. A spec can rename them with `marker_prefix`, see
// [crypto.MarkerAttributes].
const (
	AttrAlgorithm  = crypto.DefaultMarkerPrefix + ".algorithm"
	AttrKeyID      = crypto.DefaultMarkerPrefix + ".key_id"
	AttrWrappedKey = crypto.DefaultMarkerPrefix + ".wrapped_key"
)

type Decryption struct {
	*base.Stage[Spec]

	decrypter  ports.Decrypter
	algorithm  string
	attributes map[string]struct{}
	markers    crypto.Markers
	logger     *slog.Logger
}

func New(stage domain.StageSpec, validator *validator.Validate, logger *slog.Logger) (*Decryption, error) {
	generic, opts, drv, err := driver.Split(stage.Spec, drivers.NewFactory())
	if err != nil {
		return nil, err
	}
	baseStage, err := base.New[Spec](stage.Name, stage.From, generic, validator)
	if err != nil {
		return nil, err
	}
	decrypter, err := drv.NewDecrypter(opts, validator)
	if err != nil {
		return nil, err
	}

	spec := baseStage.Spec()
	algorithm := spec.Algorithm
	if algorithm == "" {
		algorithm = DefaultAlgorithm
	}
	attributes := make(map[string]struct{}, len(spec.Attributes))
	for _, name := range spec.Attributes {
		attributes[name] = struct{}{}
	}
	markers, err := crypto.MarkerAttributes(spec.MarkerPrefix)
	if err != nil {
		return nil, err
	}

	return &Decryption{
		Stage:      baseStage,
		decrypter:  decrypter,
		algorithm:  algorithm,
		attributes: attributes,
		markers:    markers,
		logger:     logger.With(slog.String("stage", stage.Name)),
	}, nil
}

// Notify decrypts the body and the listed attributes of every encrypted log
// record and passes the event on as a copy. It does not modify the event; it
// is shared with sibling branches. Records without an encryption marker pass
// through unchanged; metrics and traces pass through unchanged as well.
func (s *Decryption) Notify(ctx context.Context, event *domain.Event) error {
	mapped := *event
	if event.Logs != nil {
		logs, err := s.decryptLogs(ctx, event.Logs)
		if err != nil {
			return fmt.Errorf("%w: %w", domain.ErrEventRejected, err)
		}
		mapped.Logs = logs
	}
	return s.NotifySubscribers(ctx, &mapped)
}

func (s *Decryption) decryptLogs(ctx context.Context, data *logspb.LogsData) (*logspb.LogsData, error) {
	resources := make([]*logspb.ResourceLogs, len(data.GetResourceLogs()))
	for i, rl := range data.GetResourceLogs() {
		scopes := make([]*logspb.ScopeLogs, len(rl.GetScopeLogs()))
		for j, sl := range rl.GetScopeLogs() {
			records := make([]*logspb.LogRecord, len(sl.GetLogRecords()))
			for k, record := range sl.GetLogRecords() {
				decrypted, err := s.decryptRecord(ctx, record)
				if err != nil {
					return nil, err
				}
				records[k] = decrypted
			}
			scopes[j] = &logspb.ScopeLogs{
				Scope:      sl.GetScope(),
				SchemaUrl:  sl.GetSchemaUrl(),
				LogRecords: records,
			}
		}
		resources[i] = &logspb.ResourceLogs{
			Resource:  rl.GetResource(),
			SchemaUrl: rl.GetSchemaUrl(),
			ScopeLogs: scopes,
		}
	}
	return &logspb.LogsData{ResourceLogs: resources}, nil
}

// decryptRecord returns a copy of record with its body and the listed
// attributes decrypted and the encryption marker attributes removed. A record
// without a marker attribute is returned unchanged.
func (s *Decryption) decryptRecord(ctx context.Context, record *logspb.LogRecord) (*logspb.LogRecord, error) {
	attrs := record.GetAttributes()
	algorithm, marked := attrValue(attrs, s.markers.Algorithm)
	if !marked {
		return record, nil
	}
	if algorithm != s.algorithm {
		return nil, fmt.Errorf("%w: record is %q, stage is %q", ErrAlgorithmMismatch, algorithm, s.algorithm)
	}
	keyID, _ := attrValue(attrs, s.markers.KeyID)
	if keyID != s.decrypter.KeyID() {
		return nil, fmt.Errorf("%w: record key %q, stage key %q", ErrKeyMismatch, keyID, s.decrypter.KeyID())
	}
	wrappedKey, err := wrappedKey(attrs, s.markers.WrappedKey)
	if err != nil {
		return nil, err
	}

	body, err := s.decryptBody(ctx, record.GetBody(), wrappedKey)
	if err != nil {
		return nil, err
	}
	decryptedAttrs, err := s.decryptAttributes(ctx, attrs, wrappedKey)
	if err != nil {
		return nil, err
	}
	return &logspb.LogRecord{
		TimeUnixNano:           record.GetTimeUnixNano(),
		ObservedTimeUnixNano:   record.GetObservedTimeUnixNano(),
		SeverityNumber:         record.GetSeverityNumber(),
		SeverityText:           record.GetSeverityText(),
		Body:                   body,
		Attributes:             decryptedAttrs,
		DroppedAttributesCount: record.GetDroppedAttributesCount(),
		Flags:                  record.GetFlags(),
		TraceId:                record.GetTraceId(),
		SpanId:                 record.GetSpanId(),
		EventName:              record.GetEventName(),
	}, nil
}

// decryptBody decrypts the body of a record: a base64 string becomes a string
// again, bytes become bytes again.
func (s *Decryption) decryptBody(
	ctx context.Context,
	body *commonpb.AnyValue,
	wrappedKey []byte,
) (*commonpb.AnyValue, error) {
	if body == nil {
		return nil, nil //nolint:nilnil // A record without a body keeps none.
	}
	switch value := body.GetValue().(type) {
	case *commonpb.AnyValue_StringValue:
		plaintext, err := s.decryptString(ctx, value.StringValue, wrappedKey)
		if err != nil {
			return nil, err
		}
		return &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: plaintext}}, nil
	case *commonpb.AnyValue_BytesValue:
		plaintext, err := s.decrypt(ctx, value.BytesValue, wrappedKey)
		if err != nil {
			return nil, err
		}
		return &commonpb.AnyValue{Value: &commonpb.AnyValue_BytesValue{BytesValue: plaintext}}, nil
	default:
		return nil, ErrUnsupportedBody
	}
}

// decryptAttributes returns a copy of attrs with every listed attribute
// decrypted and the encryption marker attributes removed. A listed attribute
// that is missing is skipped; one with a non-string value rejects the event.
func (s *Decryption) decryptAttributes(
	ctx context.Context,
	attrs []*commonpb.KeyValue,
	wrappedKey []byte,
) ([]*commonpb.KeyValue, error) {
	out := make([]*commonpb.KeyValue, 0, len(attrs))
	for _, attr := range attrs {
		switch attr.GetKey() {
		case s.markers.Algorithm, s.markers.KeyID, s.markers.WrappedKey:
			continue
		}
		if _, listed := s.attributes[attr.GetKey()]; !listed {
			out = append(out, attr)
			continue
		}
		value, isString := attr.GetValue().GetValue().(*commonpb.AnyValue_StringValue)
		if !isString {
			return nil, fmt.Errorf("%w: %s", ErrNonStringAttribute, attr.GetKey())
		}
		plaintext, err := s.decryptString(ctx, value.StringValue, wrappedKey)
		if err != nil {
			return nil, err
		}
		out = append(out, stringAttr(attr.GetKey(), plaintext))
	}
	return out, nil
}

// decryptString base64-decodes encoded and decrypts it.
func (s *Decryption) decryptString(ctx context.Context, encoded string, wrappedKey []byte) (string, error) {
	ciphertext, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return "", fmt.Errorf("%w: attribute is not base64: %w", ErrDecryptionFailed, err)
	}
	plaintext, err := s.decrypt(ctx, ciphertext, wrappedKey)
	if err != nil {
		return "", err
	}
	return string(plaintext), nil
}

func (s *Decryption) decrypt(ctx context.Context, ciphertext, wrappedKey []byte) ([]byte, error) {
	plaintext, err := s.decrypter.Decrypt(ctx, wrappedKey, ciphertext)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrDecryptionFailed, err)
	}
	return plaintext, nil
}

// attrValue returns the string value of the attribute with the key.
func attrValue(attrs []*commonpb.KeyValue, key string) (string, bool) {
	for _, attr := range attrs {
		if attr.GetKey() == key {
			return attr.GetValue().GetStringValue(), true
		}
	}
	return "", false
}

// wrappedKey returns the decoded wrapped data key of the record, or nil when
// the record has none, e.g. with a local key.
func wrappedKey(attrs []*commonpb.KeyValue, attrWrappedKey string) ([]byte, error) {
	encoded, ok := attrValue(attrs, attrWrappedKey)
	if !ok || encoded == "" {
		return nil, nil
	}
	wrapped, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("%w: wrapped key is not base64: %w", ErrDecryptionFailed, err)
	}
	return wrapped, nil
}

func stringAttr(key, value string) *commonpb.KeyValue {
	return &commonpb.KeyValue{
		Key:   key,
		Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: value}},
	}
}
