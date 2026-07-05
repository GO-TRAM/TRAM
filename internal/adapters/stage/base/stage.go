// internal/adapters/stage/base/stage.go
package base

import (
	"context"
	"sync"

	"codeberg.org/eljoth/stream/internal/core/domain"
	"github.com/go-playground/validator/v10"
)

type Stage[T any] struct {
	name        string
	from        string
	subscribers []domain.Subscriber
	mu          sync.RWMutex
	spec        *T
}

func New[T any](name, from string, spec map[string]any, validator *validator.Validate) (*Stage[T], error) {
	parsedSpec, err := domain.ParseSpec(spec, new(T))
	if err != nil {
		return nil, err
	}

	if err := validator.Struct(parsedSpec); err != nil {
		return nil, err
	}

	return &Stage[T]{
		name: name,
		from: from,
	}, nil
}

func (s *Stage[T]) Name() string {
	return s.name
}

func (s *Stage[T]) From() string {
	return s.from
}

func (s *Stage[T]) Subscribe(c domain.Subscriber) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.subscribers = append(s.subscribers, c)
	return nil
}

func (s *Stage[T]) NotifySubscribers(ctx context.Context, event domain.Event) {
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
