// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

// Package rules is the contract between the validation stage and its
// drivers, e.g. cel: a driver compiles the options under its name into rules
// that decide whether the data of each signal is valid.
package rules

import (
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

// Driver compiles the options under its name into the rules of a validation.
type Driver interface {
	Compile(opts map[string]any) (Rules, error)
}

// Rules decide per signal whether its data is valid. They are safe for
// concurrent use.
type Rules interface {
	Logs(*logspb.LogsData) (bool, error)
	Metrics(*metricspb.MetricsData) (bool, error)
	Traces(*tracepb.TracesData) (bool, error)
}
