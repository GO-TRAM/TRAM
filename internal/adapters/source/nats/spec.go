package nats

type Spec struct {
	URL     string `json:"url" validate:"required"`
	Stream  string `json:"stream" validate:"required"`
	Subject string `json:"subject" validate:"required"`
	Queue   string `json:"queue,omitempty"`
	Durable string `json:"durable,omitempty"`
	Workers int    `json:"workers,omitempty" validate:"min=1 max=100"`
}
