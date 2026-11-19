// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

// Package crypto holds the registry of key drivers, the encryption stage's
// counterpart of the broker drivers: a component spec names a `driver` and
// holds the driver's options under that name, e.g. `local: {key: ...}`.
package crypto

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/go-playground/validator/v10"

	"codeberg.org/eljoth/tram/internal/ports"
)

// ErrUnknownDriver is returned when a spec names a driver the registry does
// not know.
var ErrUnknownDriver = errors.New("unknown crypto driver")

// Driver creates a [ports.Crypter] and a [ports.Decrypter] from the driver's
// options.
type Driver interface {
	NewCrypter(opts map[string]any, validator *validator.Validate) (ports.Crypter, error)
	NewDecrypter(opts map[string]any, validator *validator.Validate) (ports.Decrypter, error)
}

// DriverFactory returns the driver a spec names in its `driver` field.
type DriverFactory interface {
	Driver(name string) (Driver, error)
}

// Drivers is a fixed set of drivers by name, a [DriverFactory] for tests that
// inject another driver.
type Drivers map[string]Driver

var _ DriverFactory = Drivers(nil)

// Driver returns the driver called name.
func (d Drivers) Driver(name string) (Driver, error) {
	driver, ok := d[name]
	if !ok {
		return nil, fmt.Errorf("%w %q, available: %s", ErrUnknownDriver, name,
			strings.Join(slices.Sorted(maps.Keys(d)), ", "))
	}
	return driver, nil
}

// Split takes a component spec apart, like the broker's: the spec without the
// driver's options, the options, and the driver its `driver` field names.
// Options of another driver stay in the spec, where the component refuses
// them as unknown.
func Split(spec map[string]any, drivers DriverFactory) (map[string]any, map[string]any, Driver, error) {
	var zero Driver
	name, _ := spec["driver"].(string)
	if name == "" {
		return nil, nil, zero, errors.New("driver is required")
	}
	driver, err := drivers.Driver(name)
	if err != nil {
		return nil, nil, zero, err
	}
	raw, ok := spec[name]
	if !ok {
		return nil, nil, zero, fmt.Errorf("driver %s needs its options under %q", name, name)
	}
	opts, ok := raw.(map[string]any)
	if !ok {
		return nil, nil, zero, fmt.Errorf("options of driver %s must be an object", name)
	}
	generic := maps.Clone(spec)
	delete(generic, name)
	return generic, opts, driver, nil
}
