package core

import (
	"fmt"
	"slices"
	"strings"
	"time"

	"codeberg.org/eljoth/tram/internal/core/domain"
)

// checkHoldTimes rejects streams whose stages can hold an event for as long as
// the source waits for its delivery: such a stream fails as soon as traffic
// is too low to fill its batches. Needs the components to be created.
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
			// Collected from the sink up.
			slices.Reverse(holders)
			return fmt.Errorf(
				"%w: on the way to sink %s, %s can hold an event for %s, but source %s cancels deliveries after %s",
				domain.ErrInvalidStreamSpec, sink.Name, strings.Join(holders, ", "), held, root.Name, limit,
			)
		}
	}
	return nil
}
