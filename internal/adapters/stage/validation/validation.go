// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

// Package validation is the validation stage: it checks each signal of an
// event as a whole against the rules of its driver and drops or rejects a
// signal that does not match, as its spec says. The driver its spec names,
// e.g. cel, compiles the rules from the options under its name.
package validation

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/go-playground/validator/v10"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/go-tram/tram/internal/adapters/driver"
	"github.com/go-tram/tram/internal/adapters/stage/base"
	"github.com/go-tram/tram/internal/core/domain"
)

const (
	Type = "validation"

	OnMismatchDrop   = "drop"
	OnMismatchReject = "reject"
)

// Spec is the spec of a validation stage. The driver's options are under the
// driver's name, e.g. `cel: {logs: ...}`, and are passed to the driver.
type Spec struct {
	// Driver selects the technology of the rules, e.g. cel.
	Driver string `json:"driver" validate:"required"`
	// OnMismatch is what happens to a signal that does not match: drop
	// removes it from the event, reject rejects the whole event.
	OnMismatch string `json:"on_mismatch" validate:"required,oneof=drop reject"`
}

// Driver compiles the options under its name into the rules of a validation.
type Driver interface {
	Compile(opts map[string]any) (Rules, error)
}

// Rules decide whether a signal matches. A signal without a rule matches.
// An error rejects the event, whatever the stage does on a mismatch. Rules
// are safe for concurrent use.
type Rules interface {
	Logs(*logspb.LogsData) (bool, error)
	Metrics(*metricspb.MetricsData) (bool, error)
	Traces(*tracepb.TracesData) (bool, error)
}

type Validation struct {
	*base.Stage[Spec]

	rules  Rules
	logger *slog.Logger
}

// New creates the validation stage with the driver its spec names from
// drivers.
func New(
	stage domain.StageSpec,
	validator *validator.Validate,
	logger *slog.Logger,
	drivers driver.Factory[Driver],
) (*Validation, error) {
	generic, opts, drv, err := driver.Split(stage.Spec, drivers)
	if err != nil {
		return nil, err
	}
	b, err := base.New[Spec](stage.Name, stage.From, generic, validator)
	if err != nil {
		return nil, err
	}
	rules, err := drv.Compile(opts)
	if err != nil {
		return nil, err
	}
	return &Validation{
		Stage:  b,
		rules:  rules,
		logger: logger.With(slog.String("stage", stage.Name)),
	}, nil
}

func (v *Validation) Notify(ctx context.Context, event *domain.Event) error {
	copied := *event
	event = &copied

	if event.Logs != nil {
		keep, err := v.check(v.rules.Logs(event.Logs))
		if err != nil {
			return errors.Join(ErrLogsRejected, err)
		}
		if !keep {
			event.Logs = nil
		}
	}

	if event.Metrics != nil {
		keep, err := v.check(v.rules.Metrics(event.Metrics))
		if err != nil {
			return errors.Join(ErrMetricsRejected, err)
		}
		if !keep {
			event.Metrics = nil
		}
	}

	if event.Traces != nil {
		keep, err := v.check(v.rules.Traces(event.Traces))
		if err != nil {
			return errors.Join(ErrTracesRejected, err)
		}
		if !keep {
			event.Traces = nil
		}
	}

	if event.Logs == nil && event.Metrics == nil && event.Traces == nil {
		return nil
	}
	return v.NotifySubscribers(ctx, event)
}

// check returns whether a signal with the result of its rule stays in the
// event. A mismatch drops it or rejects the event, as the spec says; an error
// always rejects the event, since the same event fails the same way again.
func (v *Validation) check(match bool, err error) (bool, error) {
	if err != nil {
		return false, fmt.Errorf("%w: %w", domain.ErrEventRejected, err)
	}
	if match {
		return true, nil
	}
	if v.Spec().OnMismatch == OnMismatchDrop {
		return false, nil
	}
	return false, fmt.Errorf("%w: %w", domain.ErrEventRejected, ErrMismatch)
}
