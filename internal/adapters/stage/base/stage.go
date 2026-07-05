// internal/adapters/stage/base/stage.go
package base

import (
	"context"
	"sync"

	"codeberg.org/eljoth/stream/internal/core/domain"
)

type Stage struct {
	name        string
	from        string
	subscribers []domain.Subscriber
	mu          sync.RWMutex
}

func New(name, from string) *Stage {
	return &Stage{
		name: name,
		from: from,
	}
}

func (s *Stage) Name() string {
	return s.name
}

func (s *Stage) From() string {
	return s.from
}

func (s *Stage) Subscribe(c domain.Subscriber) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.subscribers = append(s.subscribers, c)
	return nil
}

func (s *Stage) NotifySubscribers(ctx context.Context, event domain.Event) {
	s.mu.RLock()
	subs := make([]domain.Subscriber, len(s.subscribers))
	copy(subs, s.subscribers)
	s.mu.RUnlock()

	p := ctx.Value("path")
	path, ok := p.(string)
	if !ok {
		path = ""
	}
	path += " -> " + s.Name()
	ctx = context.WithValue(ctx, "path", path)

	for _, sub := range subs {
		go func(c domain.Subscriber) {
			_ = c.Notify(ctx, event)
		}(sub)
	}
}
