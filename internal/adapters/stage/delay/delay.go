// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package delay

import (
	"context"
	"sync"
	"time"

	"github.com/go-playground/validator/v10"

	"codeberg.org/eljoth/tram/internal/adapters/stage/base"
	"codeberg.org/eljoth/tram/internal/core/domain"
)

const Type = "delay"

type Delay struct {
	*base.Stage[Spec]

	delay    time.Duration
	shutdown chan struct{}
	stopOnce sync.Once
}

func New(stage domain.StageSpec, validator *validator.Validate) (*Delay, error) {
	base, err := base.New[Spec](stage.Name, stage.From, stage.Spec, validator)
	if err != nil {
		return nil, err
	}
	delay, err := time.ParseDuration(base.Spec().Delay)
	if err != nil {
		return nil, err
	}
	return &Delay{
		Stage:    base,
		delay:    delay,
		shutdown: make(chan struct{}),
	}, nil
}

func (d *Delay) MaxHold() time.Duration { return d.delay }

func (d *Delay) Notify(ctx context.Context, event *domain.Event) error {
	timer := time.NewTimer(d.delay)
	defer timer.Stop()

	select {
	case <-timer.C:
	case <-ctx.Done():
		return ctx.Err()
	case <-d.shutdown:
		return ErrDelayStopped
	}

	select {
	case <-d.shutdown:
		return ErrDelayStopped
	default:
	}
	return d.NotifySubscribers(ctx, event)
}

func (d *Delay) Stop() error {
	d.stopOnce.Do(func() { close(d.shutdown) })
	return d.Stage.Stop()
}
