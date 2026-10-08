package otlp

import (
	"net/http"
	"time"

	"codeberg.org/eljoth/tram/internal/adapters/source/base"
	"codeberg.org/eljoth/tram/internal/core/domain"
)

const (
	MaxBodyBytes           = maxBodyBytes
	DefaultDeliveryTimeout = defaultDeliveryTimeout
	MaxPairwiseKeys        = maxPairwiseKeys
)

func DuplicateKey(event *domain.Event) (string, bool) {
	return duplicateKey(event)
}

func NewWithoutListener(src *base.Source[Spec], deliveryTimeout time.Duration) *OTLP {
	return &OTLP{
		Source:               src,
		deliveryTimeout:      deliveryTimeout,
		responseWriteTimeout: responseWriteTimeout,
		retryAfter:           retryAfterHeader(defaultRetryAfter),
	}
}

func RetryAfter(o *OTLP) string {
	return o.retryAfter
}

func SetResponseWriteTimeout(o *OTLP, timeout time.Duration) {
	o.responseWriteTimeout = timeout
}

func HandleLogs(o *OTLP, w http.ResponseWriter, r *http.Request) {
	o.handleLogs(w, r)
}
