package core

import "time"

// SetRestartDelays shortens the restart delays for tests. It must be called
// before the first stream is upserted.
func (c *Core) SetRestartDelays(minDelay, maxDelay time.Duration) {
	c.minRestartDelay = minDelay
	c.maxRestartDelay = maxDelay
}
