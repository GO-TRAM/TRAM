// internal/core/domain/component.go
package domain

import "context"

type Event struct {
	Data     []byte
	Metadata map[string]string
}

type Emitter interface {
	Emit(ctx context.Context, event Event) error
}

type Registry interface {
	RegisterStage(stage Stage) error
	RegisterSink(sink Sink) error
}
