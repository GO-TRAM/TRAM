package base_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/go-playground/validator/v10"

	"codeberg.org/eljoth/tram/internal/adapters/stage/base"
	"codeberg.org/eljoth/tram/internal/core/domain"
)

type noopSubscriber struct{}

func (noopSubscriber) Name() string                                { return "noop" }
func (noopSubscriber) From() string                                { return "stage" }
func (noopSubscriber) Stop() error                                 { return nil }
func (noopSubscriber) Notify(context.Context, *domain.Event) error { return nil }

// BenchmarkNotifySubscribers measures the per-event cost of fanning out to
// children, including extending the path in the context.
func BenchmarkNotifySubscribers(b *testing.B) {
	event := &domain.Event{}
	ctx := context.WithValue(context.Background(), domain.ContextKeyPath, "source")

	for _, children := range []int{1, 2, 8} {
		stage, err := base.New[struct{}]("stage", "source", nil, validator.New())
		if err != nil {
			b.Fatal(err)
		}
		for range children {
			_ = stage.Subscribe(noopSubscriber{})
		}

		b.Run(fmt.Sprintf("children=%d", children), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_ = stage.NotifySubscribers(ctx, event)
			}
		})
	}
}
