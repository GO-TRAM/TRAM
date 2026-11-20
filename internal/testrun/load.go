// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package testrun

import (
	"encoding/json"
	"fmt"
	"os"

	"codeberg.org/tram/tram/internal/adapters/configsync"
	"codeberg.org/tram/tram/internal/core/domain"
)

// streamFile is a stream as file sync reads it, plus its test cases, which
// file sync ignores.
type streamFile struct {
	configsync.StreamSpec

	Tests []domain.StreamTest `json:"tests,omitempty"`
}

// LoadStream reads a stream file: either a bare stream object or a file sync
// file that wraps it in {"stream": {...}}. The stream is read with the same
// field names as file sync, so a file runs the same in both.
func LoadStream(path string) (domain.StreamSpec, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return domain.StreamSpec{}, err
	}
	var wrapper struct {
		Stream json.RawMessage `json:"stream"`
	}
	if err = json.Unmarshal(data, &wrapper); err != nil {
		return domain.StreamSpec{}, fmt.Errorf("invalid stream file %s: %w", path, err)
	}
	if len(wrapper.Stream) > 0 {
		data = wrapper.Stream
	}
	var file streamFile
	if err = json.Unmarshal(data, &file); err != nil {
		return domain.StreamSpec{}, fmt.Errorf("invalid stream file %s: %w", path, err)
	}
	spec := (&configsync.Stream{Stream: file.StreamSpec}).AsDomain()
	spec.Tests = file.Tests
	return *spec, nil
}
