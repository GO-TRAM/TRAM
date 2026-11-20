// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package generator

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/go-playground/validator/v10"

	"github.com/go-tram/tram/internal/adapters/source/base"
	"github.com/go-tram/tram/internal/core/domain"
)

const (
	Type = "generator"
)

type Generator struct {
	*base.Source[Spec]

	interval time.Duration
	shutdown chan struct{}
	stopOnce sync.Once
}

func New(source domain.SourceSpec, validator *validator.Validate) (*Generator, error) {
	base, err := base.New[Spec](source.Name, source.Spec, validator)
	if err != nil {
		return nil, err
	}

	interval, err := time.ParseDuration(base.Spec().Interval)
	if err != nil {
		return nil, err
	}
	if interval <= 0 {
		return nil, fmt.Errorf("interval %s is not positive", base.Spec().Interval)
	}

	return &Generator{
		Source:   base,
		interval: interval,
		shutdown: make(chan struct{}),
	}, nil
}

func (g *Generator) Run(ctx context.Context) error {
	for {
		select {
		case <-g.shutdown:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(g.interval):
			event := &domain.Event{}
			_ = g.NotifySubscribers(ctx, event)
		}
	}
}

func (g *Generator) Stop() error {
	g.stopOnce.Do(func() { close(g.shutdown) })
	return nil
}
