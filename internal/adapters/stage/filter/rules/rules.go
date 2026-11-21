// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

// Package rules is the contract between the filter stage and its drivers,
// e.g. cel: a driver compiles the options under its name into rules that
// decide which records a filter keeps.
package rules

import "github.com/go-tram/tram/internal/core/domain"

// Driver compiles the options under its name into the rules of a filter.
type Driver interface {
	Compile(opts map[string]any) (Rules, error)
}

// Rules decide which records a filter keeps. They are safe for concurrent
// use.
type Rules interface {
	// Predicates returns the predicates for one event. A nil predicate keeps
	// every record of its signal. The predicates may share state, so they are
	// used for one event and by one goroutine only.
	Predicates() Predicates
}

// Predicates decide per record whether it is kept. An error rejects the
// event.
type Predicates struct {
	Logs    domain.LogFilter
	Metrics domain.MetricFilter
	Traces  domain.SpanFilter
}
