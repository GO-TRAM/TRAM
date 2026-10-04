package core

import (
	"fmt"
	"slices"
	"strings"
	"time"

	"codeberg.org/eljoth/tram/internal/core/domain"
)

// checkHoldTimes rejects a stream in which the stages on the way to a sink
// can hold an event back for as long as the source waits for its delivery.
// The source would then cancel every event that, e.g., does not fill its
// batch, so the stream would fail as soon as traffic gets low. The
// components of tree have to be created already.
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
			// Walked from the sink up, so name them in the order events take.
			slices.Reverse(holders)
			return fmt.Errorf(
				"%w: on the way to sink %s, %s can hold an event for %s, but source %s cancels deliveries after %s",
				domain.ErrInvalidStreamSpec, sink.Name, strings.Join(holders, ", "), held, root.Name, limit,
			)
		}
	}
	return nil
}
