package nats

import "errors"

var (
	ErrCELEnvironmenInitialization = errors.New("failed to initialize CEL environment")
	ErrCELInvalidLogsExpression    = errors.New("invalid logs CEL expression")
	ErrCELInvalidMetricsExpression = errors.New("invalid metrics CEL expression")
	ErrCELInvalidTracesExpression  = errors.New("invalid traces CEL expression")
)
