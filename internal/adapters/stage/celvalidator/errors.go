// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package celvalidator

import "errors"

var (
	ErrCELEnvironmenInitialization = errors.New("failed to initialize CEL environment")
	ErrCELInvalidLogsExpression    = errors.New("invalid logs CEL expression")
	ErrCELInvalidMetricsExpression = errors.New("invalid metrics CEL expression")
	ErrCELInvalidTracesExpression  = errors.New("invalid traces CEL expression")
	ErrCELRejectingLogs            = errors.New("log rejected by CEL expression")
	ErrCELRejectingMetrics         = errors.New("metric rejected by CEL expression")
	ErrCELRejectingTraces          = errors.New("trace rejected by CEL expression")
	ErrCELEvaluation               = errors.New("failed to evaluate CEL expression")
)
