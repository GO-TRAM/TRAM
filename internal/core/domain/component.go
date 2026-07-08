// internal/core/domain/component.go
package domain

import "context"

type Namable interface {
	Name() string
}

type Subscriber interface {
	Namable

	From() string
	Notify(ctx context.Context, event *Event) error
	// TODO Drain(ctx context.Context) error
}

type Observer interface {
	Namable

	Subscribe(subscriber Subscriber) error
}

type Source interface {
	Observer

	Run(ctx context.Context) error
	Stop() error
}

type Stage interface {
	Observer
	Subscriber
}

type Sink interface {
	Subscriber
}
