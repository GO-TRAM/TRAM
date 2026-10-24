// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

// Package natsconn connects the NATS sink and source so that they outlast any
// NATS outage.
package natsconn

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
)

// timing sets how often a lost connection is retried.
type timing struct {
	wait      time.Duration
	jitter    time.Duration
	jitterTLS time.Duration
}

// Jitter spreads the reconnects of the many connections a Tram replica has, so
// they do not all reach NATS at once when it comes back.
const (
	reconnectWait      = 2 * time.Second
	reconnectJitter    = time.Second
	reconnectJitterTLS = 2 * time.Second
)

// Connect connects to url. A lost connection is retried until it is closed:
// nats.go gives up after 60 attempts by default, about two minutes, and a
// connection that gave up stays closed, so a longer outage, e.g. a cluster
// update, would need a Tram restart. name identifies the connection in NATS'
// monitoring.
func Connect(url, name string, logger *slog.Logger) (*nats.Conn, error) {
	return connect(
		url,
		name,
		logger,
		timing{wait: reconnectWait, jitter: reconnectJitter, jitterTLS: reconnectJitterTLS},
	)
}

func connect(url, name string, logger *slog.Logger, t timing) (*nats.Conn, error) {
	nc, err := nats.Connect(url,
		nats.Name("tram "+name),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(t.wait),
		nats.ReconnectJitter(t.jitter, t.jitterTLS),
		// Publishing while disconnected fails at once instead of buffering, so
		// the sink's source answers 503 right away rather than after the ack
		// timeout. The buffer would also hold up to 8 MB per connection, and a
		// replica has many. Acks of the NATS source fail the same way, and
		// their messages are redelivered.
		nats.ReconnectBufSize(-1),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			logger.Warn("Disconnected from NATS, reconnecting", slog.String("connection", name), slog.Any("error", err))
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			logger.Info(
				"Reconnected to NATS",
				slog.String("connection", name),
				slog.String("url", nc.ConnectedUrlRedacted()),
			)
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}
	return nc, nil
}
