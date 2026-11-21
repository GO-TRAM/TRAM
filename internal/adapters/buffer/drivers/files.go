// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package drivers

import (
	"context"
	"log/slog"
	"time"

	"github.com/go-playground/validator/v10"

	"github.com/go-tram/tram/internal/adapters/buffer"
)

// DefaultMaxAge is the max_age of queues that do not know their own, used to
// decide when a file holds only expired entries.
const DefaultMaxAge = 24 * time.Hour

// allFiles lists the queue files of every driver in dataDir.
func allFiles(ctx context.Context, dataDir string) ([]buffer.File, error) {
	var all []buffer.File
	for _, drv := range NewFactory(validator.New(), dataDir).All() {
		found, err := drv.Files(ctx)
		if err != nil {
			return nil, err
		}
		all = append(all, found...)
	}
	return all, nil
}

// Pending counts the queue files in dataDir that still hold entries younger
// than their max_age. Files no stream refers to count too, until they
// expire: the stream may come back, and nothing is discarded before its
// max_age.
func Pending(ctx context.Context, dataDir string, logger *slog.Logger) (int, error) {
	found, err := allFiles(ctx, dataDir)
	if err != nil {
		return 0, err
	}
	now := time.Now()
	var pending int
	for _, f := range found {
		switch {
		case f.Err != nil:
			logger.WarnContext(ctx, "Skipping file that is no queue file",
				slog.String("file", f.Path), slog.Any("error", f.Err))
		case f.Info.Expired(now, DefaultMaxAge):
			if f.Info.Entries > 0 {
				logger.WarnContext(ctx, "Queue file holds only entries older than its max_age; they are not delivered",
					slog.String("file", f.Path), slog.Int("entries", f.Info.Entries))
			}
		default:
			pending++
			logger.InfoContext(ctx, "Queue file still holds entries",
				slog.String("file", f.Path), slog.Int("entries", f.Info.Entries), slog.Time("newest", f.Info.Newest))
		}
	}
	return pending, nil
}
