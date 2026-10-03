package otlp

type Spec struct {
	// URL is the OTLP HTTP endpoint to send data to (e.g., "http://localhost:4318")
	URL string `json:"url" validate:"required"`

	// Headers contains HTTP headers to send with each request
	Headers map[string]string `json:"headers,omitempty"`

	// Timeout is the HTTP request timeout as a Go duration (e.g., "5s", "30s")
	Timeout string `json:"timeout,omitempty"`

	// Insecure allows connections to servers with invalid TLS certificates
	Insecure bool `json:"insecure,omitempty"`
}
