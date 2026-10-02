package celfilter

import "errors"

var (
	ErrCELEnvironmentInitialization = errors.New("failed to initialize CEL environment")
	ErrCELInvalidLogsExpression     = errors.New("invalid logs CEL expression")
	ErrCELInvalidMetricsExpression  = errors.New("invalid metrics CEL expression")
	ErrCELInvalidTracesExpression   = errors.New("invalid traces CEL expression")
	ErrCELEvaluation                = errors.New("CEL evaluation failed")
)
