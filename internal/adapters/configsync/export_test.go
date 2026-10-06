package configsync

import "time"

// SetRetryDelays shortens the delays between retries of streams that could
// not be applied. It must be called before Run.
func SetRetryDelays(c *ConfigSync, minDelay, maxDelay time.Duration) {
	c.syncMu.Lock()
	defer c.syncMu.Unlock()
	c.minRetryDelay, c.maxRetryDelay = minDelay, maxDelay
}
