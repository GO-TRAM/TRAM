// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package ports

// Factory creates the components of one kind: sources, stages or sinks. The
// stream is the name of the stream the component belongs to; components that
// record metrics of their own label them with it.
type Factory[S, T any] interface {
	New(spec S, stream string) (T, error)
}
