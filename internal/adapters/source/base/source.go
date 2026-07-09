package base

import (
	"context"
	"sync"

	"codeberg.org/eljoth/tram/internal/core/domain"
	"github.com/go-playground/validator/v10"
	"golang.org/x/sync/errgroup"
)

type Source[T any] struct {
	ctx         context.Context
	cancel      context.CancelFunc
	name        string
	subscribers []domain.Subscriber
	mu          sync.RWMutex
	spec        *T
}

func New[T any](name string, spec map[string]any, validator *validator.Validate) (*Source[T], error) {
	ctx, cancel := context.WithCancel(context.Background())

	parsedSpec, err := domain.ParseSpec(spec, new(T))
	if err != nil {
		cancel()
		return nil, err
	}

	if err := validator.Struct(parsedSpec); err != nil {
		cancel()
		return nil, err
	}

	return &Source[T]{
		name:   name,
		ctx:    ctx,
		cancel: cancel,
		spec:   parsedSpec,
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

func (s *Source[T]) Context() context.Context {
	return s.ctx
}

func (s *Source[T]) Stop() error {
	s.cancel()
	return nil
}

func (s *Source[T]) NotifySubscribers(ctx context.Context, event *domain.Event) error {
	s.mu.RLock()
	subs := make([]domain.Subscriber, len(s.subscribers))
	copy(subs, s.subscribers)
	s.mu.RUnlock()

	ctx = context.WithValue(ctx, domain.ContextKeyPath, s.Name())

	eg := errgroup.Group{}
	for _, sub := range subs {
		eg.Go(func() error {
			return sub.Notify(ctx, event)
		})
	}
	if err := eg.Wait(); err != nil {
		return err
	}
	return nil
}
