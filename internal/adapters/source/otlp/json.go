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

// unmarshalJSON decodes an OTLP/JSON payload into msg. OTLP/JSON follows the
// protobuf JSON mapping, except that trace and span IDs are hex strings
// instead of base64, so they are converted first; protojson then decodes them
// as the bytes they stand for. Unknown fields are ignored, as the OTLP
// specification asks of receivers.
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

// isIDField reports whether key is a trace or span ID field of OTLP, under one
// of the JSON names protojson accepts. All of them are bytes fields, and no
// other field of OTLP has one of these names.
func isIDField(key string) bool {
	switch key {
	case "traceId", "spanId", "parentSpanId", "trace_id", "span_id", "parent_span_id":
		return true
	default:
		return false
	}
}
