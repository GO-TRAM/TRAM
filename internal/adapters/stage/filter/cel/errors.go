// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package cel

import "errors"

var (
	ErrEnvironmentInitialization = errors.New("failed to initialize CEL environment")
	ErrInvalidLogsExpression     = errors.New("invalid logs CEL expression")
	ErrInvalidMetricsExpression  = errors.New("invalid metrics CEL expression")
	ErrInvalidTracesExpression   = errors.New("invalid traces CEL expression")
	ErrEvaluation                = errors.New("CEL evaluation failed")
)
