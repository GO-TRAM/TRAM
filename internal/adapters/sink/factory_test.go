// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package sink_test

import (
	"log/slog"

	"github.com/go-tram/tram/internal/adapters/sink"

	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/go-tram/tram/internal/core/domain"
)

func TestFactoryUnknownType(t *testing.T) {
	t.Parallel()

	_, err := sink.NewFactory(slog.New(slog.DiscardHandler)).New(domain.SinkSpec{Name: "s", Type: "unknown"})
	require.ErrorIs(t, err, domain.ErrInvalidSinkType)
	assert.ErrorContains(t, err, `"unknown"`)
}

func TestFactoryOTLPSink(t *testing.T) {
	t.Parallel()

	factory := sink.NewFactory(slog.New(slog.DiscardHandler))
	spec := domain.SinkSpec{
		Name: "otlp-sink",
		From: "test-source",
		Type: "otlp",
		Spec: map[string]any{
			"url": "http://localhost:4318",
		},
	}

	sink, err := factory.New(spec)
	require.NoError(t, err)
	assert.NotNil(t, sink)
	assert.Equal(t, "otlp-sink", sink.Name())
	assert.Equal(t, "test-source", sink.From())
	assert.NoError(t, sink.Stop())
}
