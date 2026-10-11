// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package otlp_test

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-playground/validator/v10"
	"google.golang.org/protobuf/proto"

	"codeberg.org/eljoth/tram/internal/adapters/source/base"
	"codeberg.org/eljoth/tram/internal/adapters/source/otlp"
	"codeberg.org/eljoth/tram/internal/core/domain"
	"codeberg.org/eljoth/tram/internal/testutil/otlpgen"
)

type discardSubscriber struct{}

func (discardSubscriber) Name() string                                { return "discard" }
func (discardSubscriber) From() string                                { return "otlp" }
func (discardSubscriber) Stop() error                                 { return nil }
func (discardSubscriber) Notify(context.Context, *domain.Event) error { return nil }

func BenchmarkHandleLogs(b *testing.B) {
	shapes := append(otlpgen.Shapes(), otlpgen.Shape{Records: 10000, Resources: 10, Teams: 1, Attributes: 4})

	for _, shape := range shapes {
		body, err := proto.Marshal(otlpgen.Logs(shape))
		if err != nil {
			b.Fatal(err)
		}

		src, err := base.New[otlp.Spec]("otlp", map[string]any{"http_port": 1}, validator.New())
		if err != nil {
			b.Fatal(err)
		}
		_ = src.Subscribe(discardSubscriber{})
		o := &otlp.OTLP{Source: src}

		b.Run(shape.String(), func(b *testing.B) {
			b.SetBytes(int64(len(body)))
			b.ReportAllocs()
			for b.Loop() {
				w := httptest.NewRecorder()
				otlp.HandleLogs(o, w, httptest.NewRequest(http.MethodPost, "/v1/logs", bytes.NewReader(body)))
				if w.Code != http.StatusOK {
					b.Fatalf("status %d", w.Code)
				}
			}
			b.ReportMetric(float64(shape.Records*b.N)/b.Elapsed().Seconds(), "records/s")
		})
	}
}
