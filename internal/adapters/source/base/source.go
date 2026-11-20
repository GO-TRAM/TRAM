// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package base

import (
	"context"
	"slices"
	"sync"

	"github.com/go-playground/validator/v10"
	"golang.org/x/sync/errgroup"

	"codeberg.org/tram/tram/internal/core/domain"
)

type Source[T any] struct {
	name        string
	subscribers []domain.Subscriber
	mu          sync.RWMutex
	spec        *T
}

func New[T any](name string, spec map[string]any, validator *validator.Validate) (*Source[T], error) {
	parsedSpec, err := domain.ParseSpec(spec, new(T))
	if err != nil {
		return nil, err
	}

	if err = validator.Struct(parsedSpec); err != nil {
		return nil, err
	}

	return &Source[T]{
		name: name,
		spec: parsedSpec,
	}, nil
}

func (s *Source[T]) Name() string {
	return s.name
}

func (s *Source[T]) Spec() *T {
	return s.spec
}

func (s *Source[T]) Subscribe(c domain.Subscriber) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.subscribers = append(s.subscribers, c)
	return nil
}

func (s *Source[T]) SetSubscribers(subscribers []domain.Subscriber) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.subscribers = slices.Clone(subscribers)
}

// NotifySubscribers delivers the event to all subscribers. The read lock is
// held until every subscriber has returned, so SetSubscribers can wait for
// deliveries to the subscribers it replaces.
func (s *Source[T]) NotifySubscribers(ctx context.Context, event *domain.Event) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	ctx = context.WithValue(ctx, domain.ContextKeyPath, s.Name())

	eg := errgroup.Group{}
	for _, sub := range s.subscribers {
		eg.Go(func() error {
			return sub.Notify(ctx, event)
		})
	}
	if err := eg.Wait(); err != nil {
		return err
	}
	return nil
}
