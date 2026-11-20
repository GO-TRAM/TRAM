// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package cel_test

import (
	"log/slog"

	"github.com/go-playground/validator/v10"

	"github.com/go-tram/tram/internal/adapters/driver"
	"github.com/go-tram/tram/internal/adapters/stage/validation"
	"github.com/go-tram/tram/internal/adapters/stage/validation/cel"
	"github.com/go-tram/tram/internal/core/domain"
)

// newStage creates a validation stage with the CEL driver from a flat spec:
// on_mismatch stays in the stage spec, everything else goes under cel.
func newStage(name string, spec map[string]any) (*validation.Validation, error) {
	stage := map[string]any{"driver": cel.Name}
	opts := map[string]any{}
	for k, v := range spec {
		if k == "on_mismatch" {
			stage[k] = v
			continue
		}
		opts[k] = v
	}
	stage[cel.Name] = opts

	v := validator.New()
	return validation.New(
		domain.StageSpec{Name: name, Spec: stage},
		v,
		slog.Default(),
		driver.Set[validation.Driver]{cel.Name: cel.New(v)},
	)
}
