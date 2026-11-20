// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

// Package filter is the filter stage: it keeps the records of an event the
// rules of its driver keep and drops the rest. The driver its spec names, e.g.
// cel, compiles the rules from the options under its name; the stage walks the
// records, prunes what becomes empty and passes the rest on. The contract with
// the drivers is in package rules; only package drivers knows them.
package filter

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/go-playground/validator/v10"

	"github.com/go-tram/tram/internal/adapters/driver"
	"github.com/go-tram/tram/internal/adapters/stage/base"
	"github.com/go-tram/tram/internal/adapters/stage/filter/drivers"
	"github.com/go-tram/tram/internal/adapters/stage/filter/rules"
	"github.com/go-tram/tram/internal/core/domain"
)

const Type = "filter"

// Spec is the spec of a filter stage. The driver's options are under the
// driver's name, e.g. `cel: {logs: ...}`, and are passed to the driver.
type Spec struct {
	// Driver selects the technology of the rules, e.g. cel.
	Driver string `json:"driver" validate:"required"`
}

type Filter struct {
	*base.Stage[Spec]

	rules  rules.Rules
	logger *slog.Logger
}

// New creates the filter stage with the driver its spec names.
func New(stage domain.StageSpec, validator *validator.Validate, logger *slog.Logger) (*Filter, error) {
	return newFilter(stage, validator, logger, drivers.NewFactory(validator))
}

// newFilter is [New] with the drivers to resolve the spec's `driver` from, so
// tests can inject a driver.
func newFilter(
	stage domain.StageSpec,
	validator *validator.Validate,
	logger *slog.Logger,
	driverFactory driver.Factory[rules.Driver],
) (*Filter, error) {
	generic, opts, drv, err := driver.Split(stage.Spec, driverFactory)
	if err != nil {
		return nil, err
	}
	b, err := base.New[Spec](stage.Name, stage.From, generic, validator)
	if err != nil {
		return nil, err
	}
	compiled, err := drv.Compile(opts)
	if err != nil {
		return nil, err
	}
	return &Filter{
		Stage:  b,
		rules:  compiled,
		logger: logger.With(slog.String("stage", stage.Name)),
	}, nil
}

func (f *Filter) Notify(ctx context.Context, event *domain.Event) error {
	filtered := *event
	predicates := f.rules.Predicates()

	if predicates.Logs != nil && event.Logs != nil {
		logs, err := domain.FilterLogs(event.Logs, predicates.Logs)
		if err != nil {
			return reject(err)
		}
		filtered.Logs = logs
	}

	if predicates.Metrics != nil && event.Metrics != nil {
		metrics, err := domain.FilterMetrics(event.Metrics, predicates.Metrics)
		if err != nil {
			return reject(err)
		}
		filtered.Metrics = metrics
	}

	if predicates.Traces != nil && event.Traces != nil {
		traces, err := domain.FilterSpans(event.Traces, predicates.Traces)
		if err != nil {
			return reject(err)
		}
		filtered.Traces = traces
	}

	if filtered == *event {
		return f.NotifySubscribers(ctx, event)
	}
	if filtered.RecordCount() == 0 {
		return nil
	}
	return f.NotifySubscribers(ctx, &filtered)
}

// reject turns an error of a predicate into a rejection: the same event fails
// the same way again, so it must not be retried.
func reject(err error) error {
	return fmt.Errorf("%w: %w", domain.ErrEventRejected, err)
}
