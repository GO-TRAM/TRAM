package celfilter_test

import (
	"testing"

	"github.com/go-playground/validator/v10"

	"codeberg.org/eljoth/tram/internal/adapters/stage/celfilter"
	"codeberg.org/eljoth/tram/internal/core/domain"
	"codeberg.org/eljoth/tram/internal/testutil/otlpgen"
)

// BenchmarkCELFilter measures per-record filtering. Payloads alternate between
// two teams, so "keep-half" rebuilds containers for every scope.
func BenchmarkCELFilter(b *testing.B) {
	expressions := []struct {
		name string
		logs string
	}{
		{name: "keep-all", logs: "record.attributes.exists(a, a.key == 'team')"},
		{name: "keep-half", logs: "record.attributes.exists(a, a.key == 'team' && a.value.string_value == 'team-0')"},
		{name: "keep-none", logs: "record.attributes.exists(a, a.key == 'missing')"},
		{
			name: "resource-attribute",
			logs: "resource.attributes.exists(a, a.key == 'service.name' && a.value.string_value == 'service-0')",
		},
	}

	for _, expr := range expressions {
		filter, err := celfilter.New(
			domain.StageSpec{Name: "filter", Spec: map[string]any{"logs": expr.logs}},
			validator.New(),
		)
		if err != nil {
			b.Fatal(err)
		}

		for _, shape := range otlpgen.Shapes() {
			shape.Teams = 2
			event := &domain.Event{Logs: otlpgen.Logs(shape)}

			b.Run(expr.name+"/"+shape.String(), func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					if notifyErr := filter.Notify(b.Context(), event); notifyErr != nil {
						b.Fatal(notifyErr)
					}
				}
				b.ReportMetric(float64(shape.Records*b.N)/b.Elapsed().Seconds(), "records/s")
			})
		}
	}
}
