package otlp

type Spec struct {
	HTTPPort        int    `json:"http_port"                  validate:"required"`
	DeliveryTimeout string `json:"delivery_timeout,omitempty"`
}
