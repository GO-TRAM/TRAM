// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"time"

	// Registers the "sqlite" database/sql driver.
	_ "modernc.org/sqlite"
)

// schemaVersion is stored in PRAGMA user_version.
const schemaVersion = 1

// entry is a stored message.
type entry struct {
	id        int64
	signal    string
	payload   []byte
	createdAt time.Time
	// attempts counts the failed deliveries so far.
	attempts int
}

// stats describes what a store holds.
type stats struct {
	entries int
	bytes   int64
	// oldest and newest are zero without entries.
	oldest time.Time
	newest time.Time
}

// store is the queue in one SQLite file, written with a write-ahead log and
// synced on every commit. Payloads are kept in a table of their own, so
// scheduling a retry rewrites a few bytes instead of the payload. Times are
// passed in, the store never reads the clock.
type store struct {
	db   *sql.DB
	lock *os.File
}

// openStore opens or creates the database at path. It fails with
// buffer.ErrInUse while another process holds it.
func openStore(path string) (*store, error) {
	lock, err := acquireLock(path + ".lock")
	if err != nil {
		return nil, err
	}

	// auto_vacuum has to come before the tables are created to take effect.
	// The pragmas are applied to every connection the pool opens.
	query := url.Values{}
	for _, pragma := range []string{
		"auto_vacuum(INCREMENTAL)",
		"journal_mode(WAL)",
		"synchronous(FULL)",
		"busy_timeout(5000)",
		"foreign_keys(ON)",
	} {
		query.Add("_pragma", pragma)
	}
	dsn := url.URL{Scheme: "file", Path: path, RawQuery: query.Encode()}
	db, err := sql.Open("sqlite", dsn.String())
	if err != nil {
		return nil, errors.Join(err, releaseLock(lock))
	}
	// One connection serializes the statements in the process, so writers
	// never wait for each other inside SQLite.
	db.SetMaxOpenConns(1)

	s := &store{db: db, lock: lock}
	if err = s.migrate(context.Background()); err != nil {
		return nil, errors.Join(fmt.Errorf("prepare %s: %w", path, err), s.close())
	}
	return s, nil
}

func (s *store) migrate(ctx context.Context) error {
	var version int
	if err := s.db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return err
	}
	switch {
	case version == schemaVersion:
		return nil
	case version > schemaVersion:
		return fmt.Errorf("schema version %d is newer than %d", version, schemaVersion)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	for _, stmt := range []string{
		`CREATE TABLE IF NOT EXISTS entries (
			id           INTEGER PRIMARY KEY AUTOINCREMENT,
			signal       TEXT    NOT NULL,
			size         INTEGER NOT NULL,
			created_at   INTEGER NOT NULL,
			next_attempt INTEGER NOT NULL DEFAULT 0,
			attempts     INTEGER NOT NULL DEFAULT 0
		)`,
		`CREATE INDEX IF NOT EXISTS entries_due ON entries (next_attempt, id)`,
		`CREATE INDEX IF NOT EXISTS entries_created ON entries (created_at)`,
		`CREATE TABLE IF NOT EXISTS payloads (
			id   INTEGER PRIMARY KEY REFERENCES entries (id) ON DELETE CASCADE,
			data BLOB NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS meta (
			key   TEXT PRIMARY KEY,
			value INTEGER NOT NULL
		)`,
		fmt.Sprintf("PRAGMA user_version = %d", schemaVersion),
	} {
		if _, err = tx.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// append stores the entries in one transaction; they are durable once it
// returns without error.
func (s *store) append(ctx context.Context, entries []entry) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	insertEntry, err := tx.PrepareContext(ctx, "INSERT INTO entries (signal, size, created_at) VALUES (?, ?, ?)")
	if err != nil {
		return err
	}
	defer insertEntry.Close()
	insertPayload, err := tx.PrepareContext(ctx, "INSERT INTO payloads (id, data) VALUES (?, ?)")
	if err != nil {
		return err
	}
	defer insertPayload.Close()

	for _, e := range entries {
		res, execErr := insertEntry.ExecContext(ctx, e.signal, len(e.payload), e.createdAt.UnixMilli())
		if execErr != nil {
			return execErr
		}
		id, idErr := res.LastInsertId()
		if idErr != nil {
			return idErr
		}
		if _, execErr = insertPayload.ExecContext(ctx, id, e.payload); execErr != nil {
			return execErr
		}
	}
	return tx.Commit()
}

// due returns up to limit entries whose next attempt is not after now,
// earliest first and by order of arrival, leaving out the IDs in exclude.
func (s *store) due(ctx context.Context, now time.Time, limit int, exclude []int64) ([]entry, error) {
	ids, err := idList(exclude)
	if err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT e.id, e.signal, e.created_at, e.attempts, p.data
		FROM entries e JOIN payloads p ON p.id = e.id
		WHERE e.next_attempt <= ? AND e.id NOT IN (SELECT value FROM json_each(?))
		ORDER BY e.next_attempt, e.id
		LIMIT ?`, now.UnixMilli(), ids, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var entries []entry
	for rows.Next() {
		var e entry
		var created int64
		if err = rows.Scan(&e.id, &e.signal, &created, &e.attempts, &e.payload); err != nil {
			return nil, err
		}
		e.createdAt = time.UnixMilli(created)
		entries = append(entries, e)
	}
	return entries, rows.Err()
}

// next returns the earliest next attempt of the entries not in exclude, and
// false without such entries.
func (s *store) next(ctx context.Context, exclude []int64) (time.Time, bool, error) {
	ids, err := idList(exclude)
	if err != nil {
		return time.Time{}, false, err
	}
	var next sql.NullInt64
	err = s.db.QueryRowContext(ctx,
		"SELECT MIN(next_attempt) FROM entries WHERE id NOT IN (SELECT value FROM json_each(?))", ids).Scan(&next)
	if err != nil || !next.Valid {
		return time.Time{}, false, err
	}
	return time.UnixMilli(next.Int64), true, nil
}

// remove deletes an entry; its payload goes with it.
func (s *store) remove(ctx context.Context, id int64) error {
	_, err := s.db.ExecContext(ctx, "DELETE FROM entries WHERE id = ?", id)
	return err
}

// retry counts a failed delivery and schedules the next one.
func (s *store) retry(ctx context.Context, id int64, next time.Time) error {
	_, err := s.db.ExecContext(ctx,
		"UPDATE entries SET attempts = attempts + 1, next_attempt = ? WHERE id = ?", next.UnixMilli(), id)
	return err
}

// expire removes the entries created before cutoff, except those in exclude,
// returns how many it removed and the size of their payloads, and gives the
// freed pages back to the file system.
func (s *store) expire(ctx context.Context, cutoff time.Time, exclude []int64) (int, int64, error) {
	ids, err := idList(exclude)
	if err != nil {
		return 0, 0, err
	}
	count, size, err := s.deleteExpired(ctx, cutoff, ids)
	if err != nil {
		return 0, 0, err
	}
	// Also frees what deliveries removed since the last run.
	if _, err = s.db.ExecContext(ctx, "PRAGMA incremental_vacuum"); err != nil {
		return count, size, fmt.Errorf("vacuum: %w", err)
	}
	return count, size, nil
}

func (s *store) deleteExpired(ctx context.Context, cutoff time.Time, ids string) (int, int64, error) {
	rows, err := s.db.QueryContext(ctx, `
		DELETE FROM entries
		WHERE created_at < ? AND id NOT IN (SELECT value FROM json_each(?))
		RETURNING size`, cutoff.UnixMilli(), ids)
	if err != nil {
		return 0, 0, err
	}
	defer rows.Close()

	var count int
	var total int64
	for rows.Next() {
		var size int64
		if err = rows.Scan(&size); err != nil {
			return 0, 0, err
		}
		count++
		total += size
	}
	return count, total, rows.Err()
}

func (s *store) stats(ctx context.Context) (stats, error) {
	var st stats
	var oldest, newest sql.NullInt64
	err := s.db.QueryRowContext(ctx,
		"SELECT COUNT(*), COALESCE(SUM(size), 0), MIN(created_at), MAX(created_at) FROM entries").
		Scan(&st.entries, &st.bytes, &oldest, &newest)
	if err != nil {
		return stats{}, err
	}
	if oldest.Valid {
		st.oldest = time.UnixMilli(oldest.Int64)
		st.newest = time.UnixMilli(newest.Int64)
	}
	return st, nil
}

// setMaxAge records the max_age of the queue, so readers and drains know it.
func (s *store) setMaxAge(ctx context.Context, maxAge time.Duration) error {
	_, err := s.db.ExecContext(ctx,
		"INSERT INTO meta (key, value) VALUES ('max_age_ms', ?) ON CONFLICT (key) DO UPDATE SET value = excluded.value",
		maxAge.Milliseconds())
	return err
}

// maxAge returns the recorded max_age, and false if none was recorded.
func (s *store) maxAge(ctx context.Context) (time.Duration, bool, error) {
	var ms int64
	err := s.db.QueryRowContext(ctx, "SELECT value FROM meta WHERE key = 'max_age_ms'").Scan(&ms)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return time.Duration(ms) * time.Millisecond, true, nil
}

// close closes the database and releases the lock.
func (s *store) close() error {
	return errors.Join(s.db.Close(), releaseLock(s.lock))
}

// idList encodes IDs as a JSON array for json_each.
func idList(ids []int64) (string, error) {
	if len(ids) == 0 {
		return "[]", nil
	}
	data, err := json.Marshal(ids)
	return string(data), err
}
