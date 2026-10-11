// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package otlp

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// OTLP/JSON follows the protobuf JSON mapping, except that trace and span IDs
// are hex instead of base64, so they are converted before protojson decodes
// them.
func unmarshalJSON(body []byte, msg proto.Message) error {
	decoder := json.NewDecoder(bytes.NewReader(body))
	// Numbers stay as written, so 64-bit integers do not lose precision.
	decoder.UseNumber()

	var payload any
	if err := decoder.Decode(&payload); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return errors.New("unexpected data after JSON payload")
	}

	if err := hexIDsToBase64(payload); err != nil {
		return err
	}

	converted, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return protojson.UnmarshalOptions{DiscardUnknown: true}.Unmarshal(converted, msg)
}

func hexIDsToBase64(value any) error {
	switch v := value.(type) {
	case map[string]any:
		return hexIDsInObject(v)
	case []any:
		for _, item := range v {
			if err := hexIDsToBase64(item); err != nil {
				return err
			}
		}
	}
	return nil
}

func hexIDsInObject(object map[string]any) error {
	for key, field := range object {
		hexID, isString := field.(string)
		if !isString || !isIDField(key) {
			if err := hexIDsToBase64(field); err != nil {
				return err
			}
			continue
		}
		id, err := hex.DecodeString(hexID)
		if err != nil {
			return fmt.Errorf("%s is not a hex string: %w", key, err)
		}
		object[key] = base64.StdEncoding.EncodeToString(id)
	}
	return nil
}

func isIDField(key string) bool {
	switch key {
	case "traceId", "spanId", "parentSpanId", "trace_id", "span_id", "parent_span_id":
		return true
	default:
		return false
	}
}
