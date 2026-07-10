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
)
