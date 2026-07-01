package domain

import "errors"

var (
	ErrStageDuplicate       = errors.New("stage name used twice")
	ErrStageInvalidFrom     = errors.New("stage from is unknown to stream")
	ErrSinkDuplicate        = errors.New("sink name used twice")
	ErrSinkInvalidFrom      = errors.New("sink from is unknown to stream")
	ErrGraphNotDAG          = errors.New("stream is not a DAG")
	ErrGraphNotConnected    = errors.New("stream is not a connected graph")
	ErrGraphLeafNotSink     = errors.New("stream has a leaf that is not a sink")
	ErrGraphSinkHasChildren = errors.New("stream has a sink that has children")
	ErrInvalidStreamSpec    = errors.New("stream spec is invalid")
)
