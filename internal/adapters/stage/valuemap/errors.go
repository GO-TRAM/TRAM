// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package valuemap

import "errors"

var (
	// ErrReadingTableRef is returned when the file table_ref points to cannot
	// be read or holds no valid JSON object of string values.
	ErrReadingTableRef = errors.New("failed to read the table file")
)
