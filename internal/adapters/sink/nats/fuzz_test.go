package nats_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	natssink "codeberg.org/eljoth/tram/internal/adapters/sink/nats"
	"codeberg.org/eljoth/tram/internal/core/domain"
	"codeberg.org/eljoth/tram/internal/testutil/otlpfuzz"
)

// FuzzGroupEventsBySubjects checks that grouping partitions any payload by the
// key attribute of its log records, spans and data points. Plain go test runs
// the seeds; to search for failures, run:
//
//	go test -run '^$' -fuzz FuzzGroupEventsBySubjects ./internal/adapters/sink/nats/
func FuzzGroupEventsBySubjects(f *testing.F) {
	for _, seed := range otlpfuzz.Seeds() {
		f.Add(seed)
	}

	sink := newSink(f, map[string]any{
		"logs":    "'logs.' + record.attributes.get_attr('" + otlpfuzz.KeyAttribute + "')",
		"metrics": "'metrics.' + datapoint.attributes.get_attr('" + otlpfuzz.KeyAttribute + "')",
		"traces":  "'traces.' + span.attributes.get_attr('" + otlpfuzz.KeyAttribute + "')",
	})

	f.Fuzz(func(t *testing.T, data []byte) {
		in := otlpfuzz.Build(data)

		messages, err := natssink.GroupEventsBySubjects(sink, in.Event)
		if in.MetricWithoutDataPoints {
			require.Error(t, err, "a metric without data points has no datapoint to resolve its subject from")
			return
		}
		require.NoError(t, err)

		parts := make([]otlpfuzz.Part, len(messages))
		for i, m := range messages {
			signal, key, found := strings.Cut(m.Subject, ".")
			require.True(t, found, "subject %q", m.Subject)
			require.Equal(t, string(signalOf(m.Event)), signal, "the subject starts with the signal")
			parts[i] = otlpfuzz.Part{Key: key, Event: m.Event}
		}
		otlpfuzz.Check(t, in, parts)
	})
}

func signalOf(e *domain.Event) domain.Signal {
	switch {
	case e.Logs != nil:
		return domain.SignalLogs
	case e.Metrics != nil:
		return domain.SignalMetrics
	default:
		return domain.SignalTraces
	}
}
