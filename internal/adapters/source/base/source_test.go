package base

import (
	"context"
	"fmt"
	"testing"

	"codeberg.org/eljoth/tram/internal/core/domain"
	"github.com/go-playground/validator/v10"
)

type noopSubscriber struct{}

func (noopSubscriber) Name() string                                { return "noop" }
func (noopSubscriber) From() string                                { return "source" }
func (noopSubscriber) Stop() error                                 { return nil }
func (noopSubscriber) Notify(context.Context, *domain.Event) error { return nil }

// BenchmarkNotifySubscribers measures the per-event cost of fanning out to
// children, independent of what the children do.
func BenchmarkNotifySubscribers(b *testing.B) {
	event := &domain.Event{}

	b.Run("baseline=direct-call", func(b *testing.B) {
		var sub domain.Subscriber = noopSubscriber{}
		b.ReportAllocs()
		for b.Loop() {
			_ = sub.Notify(b.Context(), event)
		}
	})

	for _, children := range []int{1, 2, 8} {
		src, err := New[struct{}]("source", nil, validator.New())
		if err != nil {
			b.Fatal(err)
		}
		for range children {
			_ = src.Subscribe(noopSubscriber{})
		}

		b.Run(fmt.Sprintf("children=%d", children), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_ = src.NotifySubscribers(b.Context(), event)
			}
		})

		b.Run(fmt.Sprintf("children=%d/parallel", children), func(b *testing.B) {
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					_ = src.NotifySubscribers(context.Background(), event)
				}
			})
		})
	}
}
