package celvalidator

import (
	"testing"

	"codeberg.org/eljoth/tram/internal/core/domain"
	"codeberg.org/eljoth/tram/internal/testutil/otlpgen"
	"github.com/go-playground/validator/v10"
)

// BenchmarkCELValidator measures expression evaluation for different
// expression shapes and payload sizes. The stage has no subscribers, so only
// evaluation is measured.
func BenchmarkCELValidator(b *testing.B) {
	expressions := []struct {
		name string
		logs string
	}{
		// The empty expression compiles to "true" and is the lower bound.
		{name: "empty", logs: ""},
		{name: "resource-attribute", logs: "logs.resource_logs.all(r, r.resource.attributes.exists(a, a.key == 'service.name' && a.value.string_value.startsWith('service-')))"},
		{name: "record-attribute", logs: "logs.resource_logs.all(r, r.scope_logs.all(s, s.log_records.all(l, l.attributes.exists(a, a.key == 'team'))))"},
		// Fails on the first record, so it shows the cost of dropping early.
		{name: "record-attribute-drop", logs: "logs.resource_logs.all(r, r.scope_logs.all(s, s.log_records.all(l, l.attributes.exists(a, a.key == 'missing'))))"},
	}

	for _, expr := range expressions {
		cel, err := New(domain.StageSpec{
			Name: "cel",
			Spec: map[string]any{"logs": expr.logs, "on_mismatch": "drop"},
		}, validator.New())
		if err != nil {
			b.Fatal(err)
		}

		for _, shape := range otlpgen.Shapes {
			event := &domain.Event{Logs: otlpgen.Logs(shape)}
			b.Run(expr.name+"/"+shape.String(), func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					if err := cel.Notify(b.Context(), event); err != nil {
						b.Fatal(err)
					}
				}
				b.ReportMetric(float64(shape.Records*b.N)/b.Elapsed().Seconds(), "records/s")
			})
		}
	}
}
