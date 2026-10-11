// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package domain

import (
	"context"
	"time"
)

type DeliveryLimited interface {
	DeliveryTimeout() time.Duration
}

type Holding interface {
	MaxHold() time.Duration
}

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
	SetSubscribers(subscribers []Subscriber)
}

type Stage interface {
	Observer
	Subscriber
}

type Sink = Subscriber
