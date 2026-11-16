// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package nats_test

import (
	"context"
	"log/slog"
	"testing"

	natssink "codeberg.org/eljoth/tram/internal/adapters/sink/nats"

	"github.com/go-playground/validator/v10"

	"codeberg.org/eljoth/tram/internal/core/domain"
	"codeberg.org/eljoth/tram/internal/testutil/natstest"
	"codeberg.org/eljoth/tram/internal/testutil/otlpgen"
)

func BenchmarkNotify(b *testing.B) {
	url := natstest.Start(b)

	shapes := []otlpgen.Shape{
		{Records: 1, Resources: 1, Teams: 1, Attributes: 4},
		{Records: 100, Resources: 1, Teams: 1, Attributes: 4},
		{Records: 1000, Resources: 10, Teams: 1, Attributes: 4},
		{Records: 1000, Resources: 10, Teams: 10, Attributes: 4},
	}

	for _, shape := range shapes {
		sink, err := natssink.New(domain.SinkSpec{Name: "nats", From: "in", Spec: map[string]any{
			"url":     url,
			"stream":  "bench",
			"size":    float64(256 << 20),
			"max_age": "1m",
			"subject": map[string]any{
				"register": []any{"bench.>"},
				"logs":     "'bench.' + logs.resource_logs[0].scope_logs[0].log_records[0].attributes.get_attr('team')",
			},
		}}, validator.New(), slog.New(slog.DiscardHandler))
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { _ = sink.Stop() })

		event := &domain.Event{Logs: otlpgen.Logs(shape)}
		b.Run(shape.String(), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if notifyErr := sink.Notify(b.Context(), event); notifyErr != nil {
					b.Fatal(notifyErr)
				}
			}
			b.ReportMetric(float64(shape.Records*b.N)/b.Elapsed().Seconds(), "records/s")
			b.ReportMetric(float64(shape.Teams*b.N)/b.Elapsed().Seconds(), "messages/s")
		})
		// Like a source with many workers: events are sent concurrently.
		b.Run("parallel/"+shape.String(), func(b *testing.B) {
			b.ReportAllocs()
			b.SetParallelism(parallelNotifiers)
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					if notifyErr := sink.Notify(context.Background(), event); notifyErr != nil {
						b.Error(notifyErr)
						return
					}
				}
			})
			b.ReportMetric(float64(shape.Records*b.N)/b.Elapsed().Seconds(), "records/s")
			b.ReportMetric(float64(shape.Teams*b.N)/b.Elapsed().Seconds(), "messages/s")
		})
	}
}

// Notifiers per GOMAXPROCS in the parallel benchmarks.
const parallelNotifiers = 4
