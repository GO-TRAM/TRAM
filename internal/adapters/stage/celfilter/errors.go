// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package celfilter

import "errors"

var (
	ErrCELEnvironmentInitialization = errors.New("failed to initialize CEL environment")
	ErrCELInvalidLogsExpression     = errors.New("invalid logs CEL expression")
	ErrCELInvalidMetricsExpression  = errors.New("invalid metrics CEL expression")
	ErrCELInvalidTracesExpression   = errors.New("invalid traces CEL expression")
	ErrCELEvaluation                = errors.New("CEL evaluation failed")
)
