// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package nats

type Spec struct {
	URL         string      `json:"url"         validate:"required"`
	Stream      string      `json:"stream"      validate:"required"`
	Subject     SubjectSpec `json:"subject"     validate:"required"`
	Retention   string      `json:"retention"   validate:"omitempty,oneof=limits interest workqueue"`
	MaxAge      string      `json:"max_age"     validate:"omitempty"`
	Size        int64       `json:"size"`
	Compression string      `json:"compression" validate:"omitempty,oneof=none s2"`
}

type SubjectSpec struct {
	Register []string `json:"register"          validate:"required,min=1"`
	Logs     string   `json:"logs,omitempty"    validate:"required_without_all=Metrics Traces"`
	Metrics  string   `json:"metrics,omitempty" validate:"required_without_all=Logs Traces"`
	Traces   string   `json:"traces,omitempty"  validate:"required_without_all=Logs Metrics"`
}
