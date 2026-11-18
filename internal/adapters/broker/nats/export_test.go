// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package nats

import (
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
)

// Connect connects like publishers and subscribers do.
func Connect(url, name string, logger *slog.Logger) (*nats.Conn, error) {
	return connect(url, name, logger)
}

// ConnectRetryingEvery connects like Connect, but retries every wait, so a
// test can outlast nats.go's 60 default attempts quickly.
func ConnectRetryingEvery(url string, logger *slog.Logger, wait time.Duration) (*nats.Conn, error) {
	return connectWith(url, "test", logger, timing{wait: wait, jitter: time.Millisecond, jitterTLS: time.Millisecond})
}
