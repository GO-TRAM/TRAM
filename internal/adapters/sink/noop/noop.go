// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package noop

import (
	"context"
	"log/slog"

	"github.com/go-playground/validator/v10"

	"codeberg.org/eljoth/tram/internal/adapters/sink/base"
	"codeberg.org/eljoth/tram/internal/core/domain"
)

const Type = "noop"

type NoOp struct {
	*base.Sink[Spec]
}

func New(stage domain.SinkSpec, validator *validator.Validate, _ *slog.Logger) (*NoOp, error) {
	base, err := base.New[Spec](stage.Name, stage.From, stage.Spec, validator)
	if err != nil {
		return nil, err
	}
	return &NoOp{
		Sink: base,
	}, nil
}

func (o *NoOp) Notify(_ context.Context, _ *domain.Event) error {
	return nil
}
