// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package broker

import (
	"errors"
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

// Split takes a component spec apart: the spec without the driver's options,
// the options, and the driver its `driver` field names. Options of another
// driver stay in the spec, where the component refuses them as unknown.
func Split(spec map[string]any, drivers Drivers) (map[string]any, map[string]any, Driver, error) {
	name, _ := spec["driver"].(string)
	if name == "" {
		return nil, nil, nil, errors.New("driver is required")
	}
	driver, err := drivers.Get(name)
	if err != nil {
		return nil, nil, nil, err
	}
	opts, err := Options(spec, name)
	if err != nil {
		return nil, nil, nil, err
	}
	generic := maps.Clone(spec)
	delete(generic, name)
	return generic, opts, driver, nil
}
