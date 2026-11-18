// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package amqp

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/Azure/go-amqp"
)

// How long dialing, opening a session or attaching a link may take.
const connectTimeout = 10 * time.Second

// session is a connection with one session that is opened on first use and
// opened again after it failed: go-amqp does not reconnect by itself, so
// without it a broker restart would stop the stream for good.
type session struct {
	spec   ConnectionSpec
	name   string
	logger *slog.Logger

	mu   sync.Mutex
	conn *amqp.Conn
	sess *amqp.Session
	// generation counts the sessions opened, so callers can tell whether
	// the session they used is still the current one.
	generation int
}

func newSession(spec ConnectionSpec, name string, logger *slog.Logger) *session {
	return &session{spec: spec, name: name, logger: logger}
}

// get returns the current session, opening one if there is none.
func (s *session) get(ctx context.Context) (*amqp.Session, int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sess != nil {
		return s.sess, s.generation, nil
	}
	ctx, cancel := context.WithTimeout(ctx, connectTimeout)
	defer cancel()

	opts := &amqp.ConnOptions{ContainerID: "tram " + s.name, SASLType: amqp.SASLTypeAnonymous()}
	if s.spec.Username != "" {
		opts.SASLType = amqp.SASLTypePlain(s.spec.Username, s.spec.Password)
	}
	if s.spec.InsecureSkipVerify {
		//nolint:gosec // Skipping the verification is an explicit option for tests.
		opts.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: true}
	}
	conn, err := amqp.Dial(ctx, s.spec.URL, opts)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to connect to AMQP broker: %w", err)
	}
	sess, err := conn.NewSession(ctx, nil)
	if err != nil {
		_ = conn.Close()
		return nil, 0, fmt.Errorf("failed to open AMQP session: %w", err)
	}
	s.conn, s.sess = conn, sess
	s.generation++
	if s.generation > 1 {
		s.logger.InfoContext(ctx, "Reconnected to AMQP broker", slog.String("connection", s.name))
	}
	return sess, s.generation, nil
}

// fail closes the session of generation after err, if err means it is no
// longer usable, so the next get opens a new one.
func (s *session) fail(generation int, err error) {
	if !broken(err) {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if generation != s.generation || s.conn == nil {
		return
	}
	s.logger.Warn("Lost AMQP connection, reconnecting", slog.String("connection", s.name), slog.Any("error", err))
	_ = s.conn.Close()
	s.conn, s.sess = nil, nil
}

func (s *session) close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn == nil {
		return nil
	}
	err := s.conn.Close()
	s.conn, s.sess = nil, nil
	return err
}

// broken reports whether err ended the connection or session.
func broken(err error) bool {
	var connErr *amqp.ConnError
	var sessErr *amqp.SessionError
	return errors.As(err, &connErr) || errors.As(err, &sessErr)
}
