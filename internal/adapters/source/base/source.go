package base

import (
	"context"
	"sync"

	"codeberg.org/eljoth/stream/internal/core/domain"
)

type Source struct {
	name        string
	subscribers []domain.Subscriber
	mu          sync.RWMutex
}

func New(name string) *Source {
	return &Source{name: name}
}

func (s *Source) Name() string {
	return s.name
}

func (s *Source) Subscribe(c domain.Subscriber) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.subscribers = append(s.subscribers, c)
	return nil
}

func (s *Source) NotifySubscribers(ctx context.Context, event domain.Event) {
	s.mu.RLock()
	subs := make([]domain.Subscriber, len(s.subscribers))
	copy(subs, s.subscribers)
	s.mu.RUnlock()

	ctx = context.WithValue(ctx, "path", s.Name())

	for _, sub := range subs {
		go func(c domain.Subscriber) {
			_ = c.Notify(ctx, event)
		}(sub)
	}
}
