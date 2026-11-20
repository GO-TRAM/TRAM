// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package broker

import "errors"

var (
	// ErrInvalidDestination is a destination no message can be published to.
	ErrInvalidDestination = errors.New("invalid destination")
	// ErrMessageTooLarge is a message above the broker's maximum size.
	ErrMessageTooLarge = errors.New("message too large")
)
