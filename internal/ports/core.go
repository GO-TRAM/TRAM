// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package ports

import (
	"context"

	"codeberg.org/eljoth/tram/internal/core/domain"
)

type Core interface {
	UpsertStream(ctx context.Context, stream domain.StreamSpec) (*domain.StreamSpec, error)
	// UpsertResolvedStream runs resolved, which holds values like secrets that
	// stream only refers to, and shows only stream.
	UpsertResolvedStream(ctx context.Context, stream, resolved domain.StreamSpec) (*domain.StreamSpec, error)
	DeleteStream(ctx context.Context, name string) error
	GetStream(ctx context.Context, name string) (*domain.StreamSpec, error)
	GetStreams(ctx context.Context) ([]domain.StreamSpec, error)
	Watch(ctx context.Context, ch chan domain.StreamSpecWatchEvent) error
}
