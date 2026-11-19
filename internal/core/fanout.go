// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package core

import (
	"context"

	"golang.org/x/sync/errgroup"

	"codeberg.org/eljoth/tram/internal/core/domain"
)

type fanout struct {
	name     string
	children []domain.Subscriber
	out      recordCounter
}

func (f *fanout) Name() string { return f.name }
func (f *fanout) From() string { return "" }

func (f *fanout) Stop() error { return nil }

func (f *fanout) Notify(ctx context.Context, event *domain.Event) error {
	f.out.count(ctx, event)

	if len(f.children) == 1 {
		return f.children[0].Notify(ctx, event)
	}
	eg := errgroup.Group{}
	for _, child := range f.children {
		eg.Go(func() error { return child.Notify(ctx, event) })
	}
	return eg.Wait()
}
