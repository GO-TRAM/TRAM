// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package k8scrd

import "errors"

var (
	ErrEmptyNamespace        = errors.New("empty namespace in watch namespaces")
	ErrInvalidStreamSelector = errors.New("invalid stream selector")
)
