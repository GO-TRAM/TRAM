// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package cebroker_test

import (
	"testing"

	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"codeberg.org/eljoth/tram/internal/adapters/cebroker"
	"codeberg.org/eljoth/tram/internal/core/domain"
	"codeberg.org/eljoth/tram/internal/testutil/otlpgen"
)

func TestSignalOf(t *testing.T) {
	t.Parallel()

	tests := []struct {
		eventType string
		want      domain.Signal
		wantErr   bool
	}{
		{eventType: "io.opentelemetry.logs", want: domain.SignalLogs},
		{eventType: "io.opentelemetry.metrics", want: domain.SignalMetrics},
		{eventType: "io.opentelemetry.traces", want: domain.SignalTraces},
		{eventType: "io.opentelemetry.profiles", wantErr: true},
		{eventType: "logs", wantErr: true},
		{eventType: "com.example.logs", wantErr: true},
		{eventType: "", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.eventType, func(t *testing.T) {
			t.Parallel()
			got, err := cebroker.SignalOf(tt.eventType)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
			assert.Equal(t, tt.eventType, cebroker.TypeOf(got))
		})
	}
}

func TestEventRoundTrip(t *testing.T) {
	t.Parallel()

	logs := otlpgen.Logs(otlpgen.Shape{Records: 3, Resources: 1, Teams: 1, Attributes: 1})
	signal, data, err := (&domain.Event{Logs: logs}).MarshalOTLPData()
	require.NoError(t, err)

	e := cebroker.NewEvent("tram/test", "out.team-0", signal, data)
	require.NoError(t, e.Validate())
	assert.Equal(t, "io.opentelemetry.logs", e.Type())
	assert.Equal(t, "out.team-0", e.Subject())
	assert.Equal(t, cebroker.ContentType, e.DataContentType())

	got, err := cebroker.EventOf(e)
	require.NoError(t, err)
	assert.True(t, proto.Equal(logs, got.Logs))
}

func TestEventOfRejects(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		modify func(e *event.Event)
	}{
		{name: "unknown type", modify: func(e *event.Event) { e.SetType("com.example.other") }},
		{name: "other content type", modify: func(e *event.Event) { e.SetDataContentType("application/json") }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			e := cebroker.NewEvent("tram/test", "out", domain.SignalLogs, nil)
			tt.modify(e)
			_, err := cebroker.EventOf(e)
			assert.Error(t, err)
		})
	}
}
