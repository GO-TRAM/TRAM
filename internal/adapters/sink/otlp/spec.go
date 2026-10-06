package otlp

type Spec struct {
	URL string `json:"url" validate:"required"`

	Headers map[string]string `json:"headers,omitempty"`

	Timeout string `json:"timeout,omitempty"`

	Insecure bool `json:"insecure,omitempty"`
}
