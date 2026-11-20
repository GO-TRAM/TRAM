// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package validation

import "errors"

var (
	ErrMismatch        = errors.New("event did not match the validation rules")
	ErrLogsRejected    = errors.New("logs rejected by validation")
	ErrMetricsRejected = errors.New("metrics rejected by validation")
	ErrTracesRejected  = errors.New("traces rejected by validation")
)
