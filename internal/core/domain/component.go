// internal/core/domain/component.go
package domain

import "context"

type Namable interface {
	Name() string
}

type Stoppable interface {
	Stop() error
}

type Subscriber interface {
	Namable
	Stoppable

	From() string
	Notify(ctx context.Context, event *Event) error
}

type Observer interface {
	Namable
	Stoppable

	Subscribe(subscriber Subscriber) error
}

type Source interface {
	Observer

	Run(ctx context.Context) error
}

type Stage interface {
	Observer
	Subscriber
}

type Sink interface {
	Subscriber
}
