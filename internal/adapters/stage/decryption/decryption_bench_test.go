// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package decryption_test

import (
	"context"
	"log/slog"
	"testing"

	"github.com/go-playground/validator/v10"

	"codeberg.org/eljoth/tram/internal/adapters/stage/decryption"
	"codeberg.org/eljoth/tram/internal/adapters/stage/encryption"
	"codeberg.org/eljoth/tram/internal/core/domain"
	"codeberg.org/eljoth/tram/internal/testutil/otlpgen"
)

func BenchmarkNotifyLogs(b *testing.B) {
	variants := []struct {
		name       string
		attributes []string
	}{
		{name: "body-only"},
		{name: "body-plus-attributes", attributes: []string{"team", "attr-0", "attr-1", "attr-2", "attr-3"}},
	}

	for _, variant := range variants {
		enc, err := encryption.New(
			domain.StageSpec{Name: "enc", From: "source", Spec: localSpec(key, variant.attributes...)},
			validator.New(),
			slog.Default(),
		)
		if err != nil {
			b.Fatal(err)
		}
		stage, err := decryption.New(
			domain.StageSpec{Name: "dec", From: "enc", Spec: localSpec(key, variant.attributes...)},
			validator.New(),
			slog.Default(),
		)
		if err != nil {
			b.Fatal(err)
		}

		for _, shape := range otlpgen.Shapes() {
			shape.Teams = 2
			c := &capture{}
			if err := enc.Subscribe(c); err != nil {
				b.Fatal(err)
			}
			plain := &domain.Event{Logs: otlpgen.Logs(shape)}
			if err := enc.Notify(context.Background(), plain); err != nil {
				b.Fatal(err)
			}
			if len(c.events) != 1 {
				b.Fatal("expected one encrypted event")
			}
			encrypted := c.events[0]

			b.Run(variant.name+"/"+shape.String(), func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					if notifyErr := stage.Notify(b.Context(), encrypted); notifyErr != nil {
						b.Fatal(notifyErr)
					}
				}
				b.ReportMetric(float64(shape.Records*b.N)/b.Elapsed().Seconds(), "records/s")
			})
		}
	}
}
