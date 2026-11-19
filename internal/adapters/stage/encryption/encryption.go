// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

// Package encryption is a stage that encrypts the body of every log record
// and the record attributes its spec lists, so sinks send them encrypted.
// Attributes it does not list, e.g. the ones a sink's destination expression
// routes by, stay in cleartext. Metrics and traces pass through unchanged.
package encryption

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
	"codeberg.org/eljoth/tram/internal/adapters/stage/base"
	"codeberg.org/eljoth/tram/internal/core/domain"
	"codeberg.org/eljoth/tram/internal/ports"
)

// Type is the stage's type in a stream spec.
const Type = "encryption"

// DefaultAlgorithm is the payload encryption used when the spec does not name
// one.
const DefaultAlgorithm = "aes-256-gcm"

// Record attributes the stage sets on every encrypted log record, so
// consumers know how to decrypt it.
const (
	AttrAlgorithm  = "tram.encryption.algorithm"
	AttrKeyID      = "tram.encryption.key_id"
	AttrWrappedKey = "tram.encryption.wrapped_key"
)

type Encryption struct {
	*base.Stage[Spec]

	crypter    ports.Crypter
	algorithm  string
	attributes map[string]struct{}
	logger     *slog.Logger
}

func New(stage domain.StageSpec, validator *validator.Validate, logger *slog.Logger) (*Encryption, error) {
	generic, opts, driver, err := crypto.Split(stage.Spec, drivers.NewFactory())
	if err != nil {
		return nil, err
	}
	baseStage, err := base.New[Spec](stage.Name, stage.From, generic, validator)
	if err != nil {
		return nil, err
	}
	crypter, err := driver.NewCrypter(opts, validator)
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

	return &Encryption{
		Stage:      baseStage,
		crypter:    crypter,
		algorithm:  algorithm,
		attributes: attributes,
		logger:     logger.With(slog.String("stage", stage.Name)),
	}, nil
}

// Notify encrypts the body and the listed attributes of every log record and
// passes the event on as a copy. It does not modify the event; it is shared
// with sibling branches. Metrics and traces pass through unchanged.
func (s *Encryption) Notify(ctx context.Context, event *domain.Event) error {
	mapped := *event
	if event.Logs != nil {
		logs, err := s.encryptLogs(event.Logs)
		if err != nil {
			return fmt.Errorf("%w: %w", domain.ErrEventRejected, err)
		}
		mapped.Logs = logs
	}
	return s.NotifySubscribers(ctx, &mapped)
}

func (s *Encryption) encryptLogs(data *logspb.LogsData) (*logspb.LogsData, error) {
	resources := make([]*logspb.ResourceLogs, len(data.GetResourceLogs()))
	for i, rl := range data.GetResourceLogs() {
		scopes := make([]*logspb.ScopeLogs, len(rl.GetScopeLogs()))
		for j, sl := range rl.GetScopeLogs() {
			records := make([]*logspb.LogRecord, len(sl.GetLogRecords()))
			for k, record := range sl.GetLogRecords() {
				encrypted, err := s.encryptRecord(record)
				if err != nil {
					return nil, err
				}
				records[k] = encrypted
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

// encryptRecord returns a copy of record with its body and the listed
// attributes encrypted and the encryption attributes set.
func (s *Encryption) encryptRecord(record *logspb.LogRecord) (*logspb.LogRecord, error) {
	body, err := s.encryptBody(record.GetBody())
	if err != nil {
		return nil, err
	}
	attrs, err := s.encryptAttributes(record.GetAttributes())
	if err != nil {
		return nil, err
	}
	return &logspb.LogRecord{
		TimeUnixNano:           record.GetTimeUnixNano(),
		ObservedTimeUnixNano:   record.GetObservedTimeUnixNano(),
		SeverityNumber:         record.GetSeverityNumber(),
		SeverityText:           record.GetSeverityText(),
		Body:                   body,
		Attributes:             attrs,
		DroppedAttributesCount: record.GetDroppedAttributesCount(),
		Flags:                  record.GetFlags(),
		TraceId:                record.GetTraceId(),
		SpanId:                 record.GetSpanId(),
		EventName:              record.GetEventName(),
	}, nil
}

// encryptBody encrypts the body of a record: a string becomes a base64 string,
// bytes stay bytes, both holding nonce and ciphertext.
func (s *Encryption) encryptBody(body *commonpb.AnyValue) (*commonpb.AnyValue, error) {
	if body == nil {
		return nil, nil
	}
	switch value := body.GetValue().(type) {
	case *commonpb.AnyValue_StringValue:
		encrypted, err := s.encrypt([]byte(value.StringValue))
		if err != nil {
			return nil, err
		}
		return &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{
			StringValue: base64.StdEncoding.EncodeToString(encrypted),
		}}, nil
	case *commonpb.AnyValue_BytesValue:
		encrypted, err := s.encrypt(value.BytesValue)
		if err != nil {
			return nil, err
		}
		return &commonpb.AnyValue{Value: &commonpb.AnyValue_BytesValue{BytesValue: encrypted}}, nil
	default:
		return nil, ErrUnsupportedBody
	}
}

// encryptAttributes returns a copy of attrs with every listed attribute
// encrypted and the encryption attributes set. A listed attribute that is
// missing is skipped; one with a non-string value rejects the event.
func (s *Encryption) encryptAttributes(attrs []*commonpb.KeyValue) ([]*commonpb.KeyValue, error) {
	out := make([]*commonpb.KeyValue, len(attrs), len(attrs)+3)
	copy(out, attrs)
	for i, attr := range out {
		if _, listed := s.attributes[attr.GetKey()]; !listed {
			continue
		}
		value, isString := attr.GetValue().GetValue().(*commonpb.AnyValue_StringValue)
		if !isString {
			return nil, fmt.Errorf("%w: %s", ErrNonStringAttribute, attr.GetKey())
		}
		encrypted, err := s.encrypt([]byte(value.StringValue))
		if err != nil {
			return nil, err
		}
		out[i] = stringValue(attr.GetKey(), base64.StdEncoding.EncodeToString(encrypted))
	}
	out = setStringAttr(out, AttrAlgorithm, s.algorithm)
	out = setStringAttr(out, AttrKeyID, s.crypter.KeyID())
	if wrapped := s.crypter.WrappedKey(); wrapped != nil {
		out = setStringAttr(out, AttrWrappedKey, base64.StdEncoding.EncodeToString(wrapped))
	}
	return out, nil
}

func (s *Encryption) encrypt(plaintext []byte) ([]byte, error) {
	encrypted, err := s.crypter.Encrypt(plaintext)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrEncryptionFailed, err)
	}
	return encrypted, nil
}

func stringValue(key, value string) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: key, Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: value}}}
}

// setStringAttr returns a copy of attrs with the attribute key set to a string
// value, replacing an existing attribute or appending a new one.
func setStringAttr(attrs []*commonpb.KeyValue, key, value string) []*commonpb.KeyValue {
	for i, attr := range attrs {
		if attr.GetKey() == key {
			out := make([]*commonpb.KeyValue, len(attrs))
			copy(out, attrs)
			out[i] = stringValue(key, value)
			return out
		}
	}
	return append(attrs, stringValue(key, value))
}
