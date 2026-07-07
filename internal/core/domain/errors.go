package domain

import "errors"

var (
	ErrStageDuplicate       = errors.New("stage name used twice")
	ErrStageInvalidFrom     = errors.New("stage from is unknown to stream")
	ErrSinkDuplicate        = errors.New("sink name used twice")
	ErrSinkInvalidFrom      = errors.New("sink from is unknown to stream")
	ErrGraphNotTree         = errors.New("stream is not a Tree")
	ErrGraphNotConnected    = errors.New("stream is not a connected graph")
	ErrGraphLeafNotSink     = errors.New("stream has a leaf that is not a sink")
	ErrGraphSinkHasChildren = errors.New("stream has a sink that has children")
	ErrInvalidStreamSpec    = errors.New("stream spec is invalid")
	ErrInvalidSourceType    = errors.New("source type is invalid")
	ErrStreamWireStages     = errors.New("cannot wire stage")
	ErrStreamWireSinks      = errors.New("cannot wire sink")
	ErrStreamNotFound       = errors.New("stream not found")
	ErrStreamStopFailed     = errors.New("stream stop failed")
	ErrStreamWireFailed     = errors.New("stream wire failed")
	ErrStreamBuildFailed    = errors.New("stream build failed")
	ErrCoreClosed           = errors.New("core closed")
)
