package domain_test

import (
	"testing"

	"codeberg.org/eljoth/tram/internal/core/domain"
	"codeberg.org/eljoth/tram/internal/testutil/otlpgen"
)

func BenchmarkOTLPDataRoundTrip(b *testing.B) {
	signals := []struct {
		name  string
		event func(otlpgen.Shape) *domain.Event
	}{
		{name: "logs", event: func(s otlpgen.Shape) *domain.Event { return &domain.Event{Logs: otlpgen.Logs(s)} }},
		{
			name:  "metrics",
			event: func(s otlpgen.Shape) *domain.Event { return &domain.Event{Metrics: otlpgen.Metrics(s)} },
		},
		{
			name:  "traces",
			event: func(s otlpgen.Shape) *domain.Event { return &domain.Event{Traces: otlpgen.Traces(s)} },
		},
	}

	for _, signal := range signals {
		for _, shape := range otlpgen.Shapes() {
			event := signal.event(shape)
			_, data, err := event.MarshalOTLPData()
			if err != nil {
				b.Fatal(err)
			}

			b.Run(signal.name+"/"+shape.String(), func(b *testing.B) {
				b.SetBytes(int64(len(data)))
				b.ReportAllocs()
				for b.Loop() {
					sig, encoded, marshalErr := event.MarshalOTLPData()
					if marshalErr != nil {
						b.Fatal(marshalErr)
					}
					if _, unmarshalErr := domain.UnmarshalOTLPData(sig, encoded); unmarshalErr != nil {
						b.Fatal(unmarshalErr)
					}
				}
				b.ReportMetric(float64(shape.Records*b.N)/b.Elapsed().Seconds(), "records/s")
			})
		}
	}
}
