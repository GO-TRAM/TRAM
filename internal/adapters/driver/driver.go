// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

// Package driver resolves the driver a component spec names. A component that
// is a concept with exchangeable technologies, such as the broker sink or the
// filter stage, names its technology in the `driver` field and holds the
// driver's options under the driver's name, e.g.
//
//	driver: nats
//	nats: {url: ...}
//
// Fields beside them are the component's own and apply to every driver.
package driver

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
)

// ErrUnknown is a driver name no driver is registered for.
var ErrUnknown = errors.New("unknown driver")

// Factory returns the driver a spec names in its `driver` field.
type Factory[D any] interface {
	Driver(name string) (D, error)
}

// Set is a fixed set of drivers by name, a [Factory] for components whose
// drivers need no setup and for tests that inject a driver.
type Set[D any] map[string]D

var _ Factory[any] = Set[any](nil)

// Driver returns the driver called name.
func (s Set[D]) Driver(name string) (D, error) {
	driver, ok := s[name]
	if !ok {
		var zero D
		return zero, Unknown(name, slices.Sorted(maps.Keys(s))...)
	}
	return driver, nil
}

// Unknown is the error for a driver name that is not one of available.
func Unknown(name string, available ...string) error {
	return fmt.Errorf("%w %q, available: %s", ErrUnknown, name, strings.Join(available, ", "))
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
func Split[D any](spec map[string]any, drivers Factory[D]) (map[string]any, map[string]any, D, error) {
	var zero D
	name, _ := spec["driver"].(string)
	if name == "" {
		return nil, nil, zero, errors.New("driver is required")
	}
	driver, err := drivers.Driver(name)
	if err != nil {
		return nil, nil, zero, err
	}
	opts, err := Options(spec, name)
	if err != nil {
		return nil, nil, zero, err
	}
	generic := maps.Clone(spec)
	delete(generic, name)
	return generic, opts, driver, nil
}
