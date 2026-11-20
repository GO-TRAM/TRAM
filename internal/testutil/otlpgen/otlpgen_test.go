// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package otlpgen_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"codeberg.org/tram/tram/internal/testutil/otlpgen"
)

func TestShapeCounts(t *testing.T) {
	t.Parallel()

	cases := []struct {
		shape     otlpgen.Shape
		resources int
		records   int
	}{
		{shape: otlpgen.Shape{Records: 1, Resources: 1}, resources: 1, records: 1},
		{shape: otlpgen.Shape{Records: 1000, Resources: 10}, resources: 10, records: 1000},
		{shape: otlpgen.Shape{Records: 5, Resources: 0}, resources: 1, records: 5},
	}

	for _, c := range cases {
		t.Run(c.shape.String(), func(t *testing.T) {
			t.Parallel()

			logs := otlpgen.Logs(c.shape)
			assert.Len(t, logs.GetResourceLogs(), c.resources)

			records := 0
			for _, rl := range logs.GetResourceLogs() {
				for _, sl := range rl.GetScopeLogs() {
					records += len(sl.GetLogRecords())
				}
			}
			assert.Equal(t, c.records, records)
			assert.Len(t, otlpgen.Metrics(c.shape).GetResourceMetrics(), c.resources)
			assert.Len(t, otlpgen.Traces(c.shape).GetResourceSpans(), c.resources)
		})
	}
}
