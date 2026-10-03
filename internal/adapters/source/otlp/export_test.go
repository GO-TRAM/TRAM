package otlp

import (
	"net/http"
	"time"

	"codeberg.org/eljoth/tram/internal/adapters/source/base"
)

const (
	MaxBodyBytes           = maxBodyBytes
	DefaultDeliveryTimeout = defaultDeliveryTimeout
)

// NewWithoutListener builds a source around src whose handlers can be called
// directly, without a listening server.
func NewWithoutListener(src *base.Source[Spec], deliveryTimeout time.Duration) *OTLP {
	return &OTLP{Source: src, deliveryTimeout: deliveryTimeout, responseWriteTimeout: responseWriteTimeout}
}

// SetResponseWriteTimeout shortens the response write timeout for tests. It
// must be called before Run.
func SetResponseWriteTimeout(o *OTLP, timeout time.Duration) {
	o.responseWriteTimeout = timeout
}

// HandleLogs serves an OTLP/HTTP logs request without a listening server.
func HandleLogs(o *OTLP, w http.ResponseWriter, r *http.Request) {
	o.handleLogs(w, r)
}
