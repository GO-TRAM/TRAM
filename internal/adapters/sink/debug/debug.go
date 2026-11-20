// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package debug

import (
	"context"
	"log/slog"

	"github.com/go-playground/validator/v10"

	"github.com/go-tram/tram/internal/adapters/sink/base"
	"github.com/go-tram/tram/internal/core/domain"
)

const Type = "debug"

type Debug struct {
	*base.Sink[Spec]

	logger *slog.Logger
}

func New(sink domain.SinkSpec, validator *validator.Validate, logger *slog.Logger) (*Debug, error) {
	base, err := base.New[Spec](sink.Name, sink.From, sink.Spec, validator)
	if err != nil {
		return nil, err
	}
	return &Debug{
		Sink:   base,
		logger: logger,
	}, nil
}

func (d *Debug) Notify(ctx context.Context, event *domain.Event) error {
	path, _ := ctx.Value(domain.ContextKeyPath).(string)
	path += " -> " + d.Name()
	d.logger.InfoContext(ctx, "Debug event",
		slog.String("path", path), slog.Int("records", event.RecordCount()), slog.Any("event", event))
	return nil
}
