// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package broker

import (
	"fmt"
	"maps"
	"slices"
	"strings"
)

// Drivers maps driver names, the `driver` field of a spec, to drivers. The
// spec holds the driver's options under the same name.
type Drivers map[string]Driver

// Get returns the driver called name.
func (d Drivers) Get(name string) (Driver, error) {
	driver, ok := d[name]
	if !ok {
		return nil, fmt.Errorf("%w %q, available: %s", ErrUnknownDriver, name,
			strings.Join(slices.Sorted(maps.Keys(d)), ", "))
	}
	return driver, nil
}

// Options returns the options of the driver called name from a component spec
// that holds them under that name, e.g. `nats: {url: ...}`. A missing key is
// an error, so a spec cannot name one driver and configure another.
func Options(spec map[string]any, name string) (map[string]any, error) {
	raw, ok := spec[name]
	if !ok {
		return nil, fmt.Errorf("driver %s needs its options under %q", name, name)
	}
	opts, ok := raw.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("options of driver %s must be an object", name)
	}
	return opts, nil
}
