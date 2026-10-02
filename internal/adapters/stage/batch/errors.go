package batch

import "errors"

var (
	ErrBatchStopped   = errors.New("batch stopped")
	ErrInvalidTimeout = errors.New("invalid batch timeout")
)
