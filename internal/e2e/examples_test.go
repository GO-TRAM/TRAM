// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package e2e_test

import (
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-playground/validator/v10"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/yaml"

	natsdriver "codeberg.org/eljoth/tram/internal/adapters/broker/nats"
	sinkbroker "codeberg.org/eljoth/tram/internal/adapters/sink/broker"
	sinkdebug "codeberg.org/eljoth/tram/internal/adapters/sink/debug"
	sinkotlp "codeberg.org/eljoth/tram/internal/adapters/sink/otlp"
	sourcebroker "codeberg.org/eljoth/tram/internal/adapters/source/broker"
	"codeberg.org/eljoth/tram/internal/adapters/source/generator"
	sourceotlp "codeberg.org/eljoth/tram/internal/adapters/source/otlp"
	"codeberg.org/eljoth/tram/internal/adapters/stage/batch"
	"codeberg.org/eljoth/tram/internal/adapters/stage/celfilter"
	"codeberg.org/eljoth/tram/internal/adapters/stage/celvalidator"
	"codeberg.org/eljoth/tram/internal/adapters/stage/delay"
	"codeberg.org/eljoth/tram/internal/adapters/stage/noop"
	"codeberg.org/eljoth/tram/internal/core/domain"
)

type exampleComponent struct {
	Name string         `json:"name"`
	Type string         `json:"type"`
	Spec map[string]any `json:"spec"`
}

type exampleStream struct {
	Source exampleComponent   `json:"source"`
	Stages []exampleComponent `json:"stages"`
	Sinks  []exampleComponent `json:"sinks"`
}

func TestExamplesAreValid(t *testing.T) {
	t.Parallel()

	configs, err := filepath.Glob("../../setup/config/*.json")
	require.NoError(t, err)
	require.NotEmpty(t, configs)
	for _, path := range configs {
		t.Run(filepath.Base(path), func(t *testing.T) {
			t.Parallel()

			var file struct {
				Stream exampleStream `json:"stream"`
			}
			require.NoError(t, json.Unmarshal(readFile(t, path), &file))
			checkExample(t, file.Stream)
		})
	}

	manifests, err := filepath.Glob("../../setup/k8s/*.yaml")
	require.NoError(t, err)
	require.NotEmpty(t, manifests)
	for _, path := range manifests {
		t.Run(filepath.Base(path), func(t *testing.T) {
			t.Parallel()

			var manifest struct {
				Metadata struct {
					Name string `json:"name"`
				} `json:"metadata"`
				Spec exampleStream `json:"spec"`
			}
			require.NoError(t, yaml.Unmarshal(readFile(t, path), &manifest))
			assert.Empty(t, validation.IsDNS1123Subdomain(manifest.Metadata.Name),
				"Kubernetes rejects the name %q", manifest.Metadata.Name)
			checkExample(t, manifest.Spec)
		})
	}
}

func readFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	return data
}

func checkExample(t *testing.T, stream exampleStream) {
	t.Helper()

	check := func(kind string, c exampleComponent, parse func(string, map[string]any) error) {
		assert.NoError(t, parse(c.Type, c.Spec), "%s %s", kind, c.Name)
	}
	check("source", stream.Source, parseSource)
	for _, stage := range stream.Stages {
		check("stage", stage, parseStage)
	}
	for _, sink := range stream.Sinks {
		check("sink", sink, parseSink)
	}
}

func parse[T any](raw map[string]any) error {
	spec, err := domain.ParseSpec(raw, new(T))
	if err != nil {
		return err
	}
	return validator.New(validator.WithRequiredStructEnabled()).Struct(spec)
}

func parseSource(typ string, raw map[string]any) error {
	switch typ {
	case sourceotlp.Type:
		return parse[sourceotlp.Spec](raw)
	case sourcebroker.Type:
		return parseBroker[sourcebroker.Spec, natsdriver.SubscriberSpec](raw)
	case generator.Type:
		return parse[generator.Spec](raw)
	default:
		return unknownType(typ)
	}
}

func parseStage(typ string, raw map[string]any) error {
	switch typ {
	case batch.Type:
		return parse[batch.Spec](raw)
	case celfilter.Type:
		return parse[celfilter.Spec](raw)
	case celvalidator.Type:
		return parse[celvalidator.Spec](raw)
	case delay.Type:
		return parse[delay.Spec](raw)
	case noop.Type:
		return parse[noop.Spec](raw)
	default:
		return unknownType(typ)
	}
}

func parseSink(typ string, raw map[string]any) error {
	switch typ {
	case sinkdebug.Type:
		return parse[sinkdebug.Spec](raw)
	case sinkbroker.Type:
		return parseBroker[sinkbroker.Spec, natsdriver.PublisherSpec](raw)
	case sinkotlp.Type:
		return parse[sinkotlp.Spec](raw)
	default:
		return unknownType(typ)
	}
}

// parseBroker checks a broker spec: the generic fields as T, the options of
// the NATS driver, the only driver, as D.
func parseBroker[T, D any](raw map[string]any) error {
	if raw["driver"] != natsdriver.Name {
		return fmt.Errorf("unknown broker driver %v", raw["driver"])
	}
	opts, ok := raw[natsdriver.Name].(map[string]any)
	if !ok {
		return fmt.Errorf("missing options of driver %s", natsdriver.Name)
	}
	generic := maps.Clone(raw)
	delete(generic, natsdriver.Name)
	if err := parse[T](generic); err != nil {
		return err
	}
	return parse[D](opts)
}

func unknownType(typ string) error {
	return fmt.Errorf("unknown component type %q", typ)
}
