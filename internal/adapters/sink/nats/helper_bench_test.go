package nats

import (
	"testing"

	"codeberg.org/eljoth/tram/internal/adapters/sink/base"
	"codeberg.org/eljoth/tram/internal/core/domain"
	"codeberg.org/eljoth/tram/internal/testutil/otlpgen"
	"github.com/go-playground/validator/v10"
)

// groupingShapes vary the dimensions grouping cost depends on: records to
// evaluate and clone, resources to match, and subjects to group into.
var groupingShapes = []otlpgen.Shape{
	{Records: 100, Resources: 1, Teams: 1, Attributes: 4},
	{Records: 100, Resources: 10, Teams: 10, Attributes: 4},
	{Records: 1000, Resources: 1, Teams: 1, Attributes: 4},
	{Records: 1000, Resources: 1, Teams: 10, Attributes: 4},
	{Records: 1000, Resources: 10, Teams: 1, Attributes: 4},
	{Records: 1000, Resources: 10, Teams: 10, Attributes: 4},
	{Records: 1000, Resources: 100, Teams: 1, Attributes: 4},
	{Records: 1000, Resources: 100, Teams: 10, Attributes: 4},
}

// newGroupingSink builds a sink without a NATS connection, which grouping
// does not need.
func newGroupingSink(b testing.TB, logsSubject string) *NATS {
	b.Helper()
	subject := map[string]any{"register": []any{"logs.>"}, "logs": logsSubject}
	sinkBase, err := base.New[Spec]("nats", "in", map[string]any{
		"url":     "nats://localhost:4222",
		"stream":  "bench",
		"subject": subject,
	}, validator.New())
	if err != nil {
		b.Fatal(err)
	}
	logs, metrics, traces, err := compileSubjects(sinkBase.Spec().Subject)
	if err != nil {
		b.Fatal(err)
	}
	return &NATS{Sink: sinkBase, logs: logs, metrics: metrics, traces: traces}
}

// BenchmarkGroupEventsBySubjects measures splitting a batch into one batch per
// subject, which the sink does for every event before publishing.
func BenchmarkGroupEventsBySubjects(b *testing.B) {
	sink := newGroupingSink(b, "'logs.' + logs.resource_logs[0].scope_logs[0].log_records[0].attributes.get_attr('team')")

	for _, shape := range groupingShapes {
		event := &domain.Event{Logs: otlpgen.Logs(shape)}

		grouped, err := sink.groupEventsBySubjects(event)
		if err != nil {
			b.Fatal(err)
		}
		if len(grouped) != shape.Teams {
			b.Fatalf("got %d subjects, want %d", len(grouped), shape.Teams)
		}

		b.Run(shape.String(), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, err := sink.groupEventsBySubjects(event); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(shape.Records*b.N)/b.Elapsed().Seconds(), "records/s")
		})
	}
}
