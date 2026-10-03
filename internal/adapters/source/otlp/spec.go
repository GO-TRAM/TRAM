package otlp

type Spec struct {
	HTTPPort int `json:"http_port" validate:"required"`
	// DeliveryTimeout bounds passing a request on to the stages and sinks, as
	// a Go duration. A request that takes longer is cancelled and answered
	// with 503, which clients retry. Defaults to 8s.
	DeliveryTimeout string `json:"delivery_timeout,omitempty"`
}
