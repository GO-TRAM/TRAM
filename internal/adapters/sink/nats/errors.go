// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package nats

import (
	"errors"

	"codeberg.org/eljoth/tram/internal/adapters/sink/celentry"
)

var (
	ErrCELEnvironmenInitialization = celentry.ErrEnvironment
	ErrCELInvalidLogsExpression    = celentry.ErrInvalidLogsExpression
	ErrCELInvalidMetricsExpression = celentry.ErrInvalidMetricsExpression
	ErrCELInvalidTracesExpression  = celentry.ErrInvalidTracesExpression
	ErrStorageMismatch             = errors.New("stream has a different storage, which JetStream cannot change")
	ErrInvalidPartition            = errors.New("invalid partition configuration")
)
