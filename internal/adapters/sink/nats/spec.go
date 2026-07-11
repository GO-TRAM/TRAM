package nats

type Spec struct {
	URL     string `json:"url" validate:"required"`
	Stream  string `json:"stream" validate:"required"`
	Subject string `json:"subject" validate:"required"`
}
