// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

// Package natspartition spreads keys over a fixed number of JetStream streams.
// The NATS sink and source share it, so a consumer finds the stream its key
// was published to.
package natspartition

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

const (
	// Placeholder is replaced by the partition number in stream names and
	// subjects.
	Placeholder = "{partition}"

	// MetadataCount and MetadataIndex are the stream metadata keys holding the
	// partition count and the stream's partition.
	MetadataCount = "tram.partition.count"
	MetadataIndex = "tram.partition.index"

	MaxCount = 1024
)

var ErrMismatch = errors.New("stream belongs to another partitioning")

// FNV-1a, 32 bit. Changing it moves keys to other streams, so it must stay.
const (
	fnvOffset = 2166136261
	fnvPrime  = 16777619
)

// Of returns the partition of key, from 0 to count-1.
func Of(key string, count int) int {
	h := uint32(fnvOffset)
	for i := range len(key) {
		h ^= uint32(key[i])
		h *= fnvPrime
	}
	return int(h % uint32(count)) //nolint:gosec // count is between 1 and MaxCount.
}

// Expand replaces the placeholder in s with partition.
func Expand(s string, partition int) string {
	return strings.ReplaceAll(s, Placeholder, strconv.Itoa(partition))
}

func HasPlaceholder(s string) bool {
	return strings.Contains(s, Placeholder)
}

// Metadata returns the stream metadata of partition.
func Metadata(partition, count int) map[string]string {
	return map[string]string{
		MetadataCount: strconv.Itoa(count),
		MetadataIndex: strconv.Itoa(partition),
	}
}

// Check returns an error if metadata belongs to another partition or count.
// Metadata without partition keys passes, e.g. a stream set up outside Tram.
func Check(stream string, metadata map[string]string, partition, count int) error {
	for key, want := range Metadata(partition, count) {
		if got, ok := metadata[key]; ok && got != want {
			return fmt.Errorf("%w: stream %s has %s %s, expected %s", ErrMismatch, stream, key, got, want)
		}
	}
	return nil
}
