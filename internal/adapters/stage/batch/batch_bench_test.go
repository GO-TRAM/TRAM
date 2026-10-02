package batch

import (
	"context"
	"sync/atomic"
	"testing"

	"codeberg.org/eljoth/tram/internal/core/domain"
	"codeberg.org/eljoth/tram/internal/testutil/otlpgen"
	"github.com/go-playground/validator/v10"
)

type countingSubscriber struct {
	batches atomic.Int64
}

func (c *countingSubscriber) Name() string { return "counter" }
func (c *countingSubscriber) From() string { return "batch" }
func (c *countingSubscriber) Stop() error  { return nil }
func (c *countingSubscriber) Notify(context.Context, *domain.Event) error {
	c.batches.Add(1)
	return nil
}

func newBenchBatch(b *testing.B, spec map[string]any) (*Batch, *countingSubscriber) {
	b.Helper()
	batch, err := New(domain.StageSpec{Name: "batch", From: "source", Spec: spec}, validator.New())
	if err != nil {
		b.Fatal(err)
	}
	sub := &countingSubscriber{}
	_ = batch.Subscribe(sub)
	b.Cleanup(func() { _ = batch.Stop() })
	return batch, sub
}

// BenchmarkBatch measures the cost of passing events through the batch stage.
func BenchmarkBatch(b *testing.B) {
	for _, shape := range otlpgen.Shapes {
		event := &domain.Event{Logs: otlpgen.Logs(shape)}

		// Every event fills a batch on its own, so this is the overhead of
		// the stage without any merging.
		b.Run("flush-per-event/"+shape.String(), func(b *testing.B) {
			batch, _ := newBenchBatch(b, map[string]any{"max_records": shape.Records, "timeout": "1h"})
			b.ReportAllocs()
			for b.Loop() {
				if err := batch.Notify(b.Context(), event); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(shape.Records*b.N)/b.Elapsed().Seconds(), "records/s")
		})

		// Concurrent callers share batches of up to 8 events, like parallel
		// OTLP requests or NATS workers would.
		b.Run("parallel-8-events/"+shape.String(), func(b *testing.B) {
			batch, sub := newBenchBatch(b, map[string]any{"max_records": 8 * shape.Records, "timeout": "1ms"})
			b.SetParallelism(8)
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					if err := batch.Notify(context.Background(), event); err != nil {
						b.Error(err)
						return
					}
				}
			})
			b.ReportMetric(float64(shape.Records*b.N)/b.Elapsed().Seconds(), "records/s")
			b.ReportMetric(float64(b.N)/float64(max(1, sub.batches.Load())), "events/batch")
		})
	}
}
