package nats

import (
	"testing"

	"codeberg.org/eljoth/tram/internal/core/domain"
	"codeberg.org/eljoth/tram/internal/testutil/natstest"
	"codeberg.org/eljoth/tram/internal/testutil/otlpgen"
	"github.com/go-playground/validator/v10"
)

// BenchmarkNotify measures the whole sink against a NATS server in a
// container: grouping records by subject and publishing one message per
// subject, waiting for JetStream's acknowledgement each time. Compare with
// BenchmarkGroupEventsBySubjects to see the share of publishing.
//
// Requires Docker or Podman; skipped otherwise.
func BenchmarkNotify(b *testing.B) {
	url := natstest.Start(b)

	shapes := []otlpgen.Shape{
		{Records: 1, Resources: 1, Teams: 1, Attributes: 4},
		{Records: 100, Resources: 1, Teams: 1, Attributes: 4},
		{Records: 1000, Resources: 10, Teams: 1, Attributes: 4},
		// 10 subjects per event, so 10 messages are published per Notify.
		{Records: 1000, Resources: 10, Teams: 10, Attributes: 4},
	}

	for _, shape := range shapes {
		sink, err := New(domain.SinkSpec{Name: "nats", From: "in", Spec: map[string]any{
			"url":    url,
			"stream": "bench",
			// Bounds the container's storage; old messages are discarded.
			"size":      float64(256 << 20),
			"retention": "1m",
			"subject": map[string]any{
				"register": []any{"bench.>"},
				"logs":     "'bench.' + logs.resource_logs[0].scope_logs[0].log_records[0].attributes.get_attr('team')",
			},
		}}, validator.New())
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { _ = sink.Stop() })

		event := &domain.Event{Logs: otlpgen.Logs(shape)}
		b.Run(shape.String(), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if err := sink.Notify(b.Context(), event); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(shape.Records*b.N)/b.Elapsed().Seconds(), "records/s")
			b.ReportMetric(float64(shape.Teams*b.N)/b.Elapsed().Seconds(), "messages/s")
		})
	}
}
