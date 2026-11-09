// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package valuemap

const (
	// LevelRecord reads the source attribute from the record itself, the
	// default.
	LevelRecord = "record"
	// LevelResource reads the source attribute from the record's resource.
	LevelResource = "resource"
)

type Spec struct {
	// SourceAttr is the attribute whose value is looked up.
	SourceAttr string `json:"source_attr" validate:"required"`
	// TargetAttr is the attribute set to the result of the lookup.
	TargetAttr string `json:"target_attr" validate:"required"`
	// Default is the value set on a lookup miss and on a missing source
	// attribute.
	Default string `json:"default" validate:"required"`
	// Level is the level the source attribute is read from: record (default)
	// or resource.
	Level string `json:"level,omitempty" validate:"omitempty,oneof=record resource"`
	// Table holds the lookup table, source value to target value. Of table
	// and table_ref, exactly one has to be set.
	Table map[string]string `json:"table,omitempty" validate:"required_without=TableRef,excluded_with=TableRef"`
	// TableRef points to a JSON file that holds the lookup table, read once
	// when the stage is created. Of table and table_ref, exactly one has to
	// be set.
	TableRef string `json:"table_ref,omitempty" validate:"required_without=Table,excluded_with=Table"`
}
