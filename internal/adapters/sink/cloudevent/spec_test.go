// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package cloudevent_test

import (
	"log/slog"
	"testing"

	"github.com/go-playground/validator/v10"
	"github.com/stretchr/testify/assert"

	"codeberg.org/eljoth/tram/internal/adapters/sink/cloudevent"
	"codeberg.org/eljoth/tram/internal/core/domain"
)

// The cases fail before a connection is attempted.
func TestNewRejectsInvalidSpec(t *testing.T) {
	t.Parallel()

	nats := map[string]any{"url": "nats://127.0.0.1:1"}
	logs := map[string]any{"logs": "'out'"}
	tests := []struct {
		name string
		spec map[string]any
	}{
		{name: "missing broker", spec: map[string]any{"destination": logs, "nats": nats}},
		{name: "unknown broker", spec: map[string]any{"broker": "kafka", "destination": logs, "nats": nats}},
		{name: "nats without nats spec", spec: map[string]any{"broker": "nats", "destination": logs}},
		{name: "nats without url", spec: map[string]any{
			"broker": "nats", "destination": logs, "nats": map[string]any{},
		}},
		{name: "no destination", spec: map[string]any{"broker": "nats", "nats": nats}},
		{name: "empty destination", spec: map[string]any{
			"broker": "nats", "destination": map[string]any{}, "nats": nats,
		}},
		{name: "invalid CEL", spec: map[string]any{
			"broker": "nats", "destination": map[string]any{"logs": "'out' +"}, "nats": nats,
		}},
		{name: "negative concurrency", spec: map[string]any{
			"broker": "nats", "destination": logs, "nats": nats, "concurrency": float64(-1),
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := cloudevent.New(domain.SinkSpec{Name: "ce", From: "in", Spec: tt.spec},
				validator.New(validator.WithRequiredStructEnabled()), slog.New(slog.DiscardHandler))
			assert.Error(t, err)
		})
	}
}
