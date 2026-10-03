// Package domain contains Tram's core types: streams, their components and
// the events flowing through them.
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
	// SetSubscribers replaces all subscribers of a running source. It returns
	// once no event is being delivered to the previous subscribers anymore, so
	// they can be stopped afterwards. Events arriving meanwhile wait and go to
	// the new subscribers.
	SetSubscribers(subscribers []Subscriber)
}

type Stage interface {
	Observer
	Subscriber
}

// Sink is a subscriber at the end of a stream; it needs nothing beyond the
// Subscriber methods.
type Sink = Subscriber
