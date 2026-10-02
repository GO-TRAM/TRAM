package otlpgen

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestShapeCounts(t *testing.T) {
	cases := []struct {
		shape     Shape
		resources int
		records   int
	}{
		{shape: Shape{Records: 1, Resources: 1}, resources: 1, records: 1},
		{shape: Shape{Records: 1000, Resources: 10}, resources: 10, records: 1000},
		{shape: Shape{Records: 5, Resources: 0}, resources: 1, records: 5},
	}

	for _, c := range cases {
		t.Run(c.shape.String(), func(t *testing.T) {
			logs := Logs(c.shape)
			assert.Len(t, logs.ResourceLogs, c.resources)

			records := 0
			for _, rl := range logs.ResourceLogs {
				for _, sl := range rl.ScopeLogs {
					records += len(sl.LogRecords)
				}
			}
			assert.Equal(t, c.records, records)
			assert.Len(t, Metrics(c.shape).ResourceMetrics, c.resources)
			assert.Len(t, Traces(c.shape).ResourceSpans, c.resources)
		})
	}
}
