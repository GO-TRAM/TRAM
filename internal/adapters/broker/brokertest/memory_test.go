// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package brokertest_test

import (
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/go-tram/tram/internal/adapters/broker/brokertest"
)

func TestMemoryConformance(t *testing.T) {
	t.Parallel()

	m := brokertest.NewMemory(1024)
	var n atomic.Int64
	brokertest.Run(t, brokertest.Harness{
		Driver: m,
		Setup: func(*testing.T) brokertest.Setup {
			dest := fmt.Sprintf("dest.%d", n.Add(1))
			return brokertest.Setup{
				Destination: dest,
				Publisher:   map[string]any{},
				Subscriber:  map[string]any{"destination": dest, "durable": "d"},
			}
		},
		InvalidDestinations: []string{"", "a b", "a.*", "a.>"},
	})
}
