// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"fmt"
	"log/slog"

	"github.com/spf13/cobra"

	"github.com/go-tram/tram/internal/adapters/stage"
	"github.com/go-tram/tram/internal/core"
	"github.com/go-tram/tram/internal/testrun"
)

type testOptions struct {
	secretsFile string
}

func newTestCmd(logger *slog.Logger) *cobra.Command {
	var opts testOptions
	cmd := &cobra.Command{
		Use:   "test <stream.json>",
		Short: "Run the test cases of a stream file locally",
		Long: `Run the test cases of a stream file locally.

The file holds either a bare stream object or a file sync file that wraps it
in {"stream": {...}}, like the examples in examples/config/.

The stream is loaded into its own in-memory tree: the source is replaced by
a test source that emits the test inputs and every sink by a capture sink, so
no external system is contacted. Stages are the real stages and process the
events. References in stage specs are resolved from the secrets file, keyed
"<name>/<key>".`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runTest(cmd, logger, opts, args[0])
		},
	}
	cmd.Flags().StringVar(&opts.secretsFile, "secrets", "",
		"JSON file with secret values keyed \"<name>/<key>\" for references in stage specs")
	return cmd
}

func runTest(cmd *cobra.Command, logger *slog.Logger, opts testOptions, path string) error {
	spec, err := testrun.LoadStream(path)
	if err != nil {
		return err
	}
	if len(spec.Tests) == 0 {
		return fmt.Errorf("stream %s has no tests", spec.Name)
	}

	var secrets map[string]string
	if opts.secretsFile != "" {
		secrets, err = testrun.LoadSecrets(opts.secretsFile)
		if err != nil {
			return err
		}
	}
	prepared, err := testrun.Prepare(spec, secrets)
	if err != nil {
		return err
	}

	results, err := core.RunStreamTests(cmd.Context(), prepared, stage.NewFactory(logger), logger)
	if err != nil {
		return err
	}

	failed := 0
	out := cmd.OutOrStdout()
	for _, result := range results {
		if result.Passed {
			fmt.Fprintf(out, "PASS %s\n", result.Name)
			continue
		}
		failed++
		fmt.Fprintf(out, "FAIL %s\n", result.Name)
		for _, problem := range result.Problems {
			fmt.Fprintf(out, "  %s\n", problem)
		}
	}
	fmt.Fprintf(out, "%d/%d tests passed\n", len(results)-failed, len(results))
	if failed > 0 {
		return fmt.Errorf("%d of %d tests failed", failed, len(results))
	}
	return nil
}
