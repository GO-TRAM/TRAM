// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-tram/tram/internal/adapters/buffer"
)

// inspect reads what a database file holds, read-only and without its lock,
// so it does not disturb a queue using the file.
func inspect(ctx context.Context, path string) (buffer.Info, error) {
	query := url.Values{}
	query.Add("mode", "ro")
	query.Add("_pragma", "busy_timeout(5000)")
	dsn := url.URL{Scheme: "file", Path: path, RawQuery: query.Encode()}
	db, err := sql.Open("sqlite", dsn.String())
	if err != nil {
		return buffer.Info{}, err
	}
	defer db.Close()

	var info buffer.Info
	var newest sql.NullInt64
	if err = db.QueryRowContext(ctx, "SELECT COUNT(*), MAX(created_at) FROM entries").
		Scan(&info.Entries, &newest); err != nil {
		return buffer.Info{}, fmt.Errorf("inspect %s: %w", path, err)
	}
	if newest.Valid {
		info.Newest = time.UnixMilli(newest.Int64)
	}
	var ms int64
	err = db.QueryRowContext(ctx, "SELECT value FROM meta WHERE key = 'max_age_ms'").Scan(&ms)
	switch {
	case err == nil:
		info.MaxAge = time.Duration(ms) * time.Millisecond
	case !errors.Is(err, sql.ErrNoRows):
		return buffer.Info{}, fmt.Errorf("inspect %s: %w", path, err)
	}
	return info, nil
}

// queuePaths returns the queue files in dir, without the write-ahead log,
// shared memory and lock files next to them.
func queuePaths(dir string) ([]string, error) {
	var paths []string
	err := filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".db") {
			return nil
		}
		paths = append(paths, path)
		return nil
	})
	return paths, err
}
