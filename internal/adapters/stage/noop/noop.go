// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package noop

import (
	"context"

	"github.com/go-playground/validator/v10"

	"codeberg.org/eljoth/tram/internal/adapters/stage/base"
	"codeberg.org/eljoth/tram/internal/core/domain"
)

const Type = "noop"

type NoOp struct {
	*base.Stage[Spec]
}

func New(stage domain.StageSpec, validator *validator.Validate) (*NoOp, error) {
	base, err := base.New[Spec](stage.Name, stage.From, stage.Spec, validator)
	if err != nil {
		return nil, err
	}
	return &NoOp{
		Stage: base,
	}, nil
}

func (n *NoOp) Notify(ctx context.Context, event *domain.Event) error {
	return n.NotifySubscribers(ctx, event)
}
