// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package batch

import "errors"

var (
	ErrBatchStopped   = errors.New("batch stopped")
	ErrInvalidTimeout = errors.New("invalid batch timeout")
)
