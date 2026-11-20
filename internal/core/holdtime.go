// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package core

import (
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/go-tram/tram/internal/core/domain"
)

func checkHoldTimes(tree *domain.Tree) error {
	root := tree.Root()
	source, err := root.GetSource()
	if err != nil {
		return err
	}
	limited, ok := source.(domain.DeliveryLimited)
	if !ok {
		return nil
	}
	limit := limited.DeliveryTimeout()

	for _, sink := range tree.LeafNodes() {
		var held time.Duration
		var holders []string
		for node := sink.Parent; node != nil && node != root; node = node.Parent {
			stage, stageErr := node.GetStage()
			if stageErr != nil {
				return stageErr
			}
			if holding, isHolding := stage.(domain.Holding); isHolding {
				held += holding.MaxHold()
				holders = append(holders, fmt.Sprintf("%s (%s)", node.Name, holding.MaxHold()))
			}
		}
		if held >= limit {
			slices.Reverse(holders)
			return fmt.Errorf(
				"%w: on the way to sink %s, %s can hold an event for %s, but source %s cancels deliveries after %s",
				domain.ErrInvalidStreamSpec, sink.Name, strings.Join(holders, ", "), held, root.Name, limit,
			)
		}
	}
	return nil
}
