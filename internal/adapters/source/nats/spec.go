package nats

type Spec struct {
	URL     string `json:"url"               validate:"required"`
	Stream  string `json:"stream"            validate:"required"`
	Subject string `json:"subject"           validate:"required"`
	Durable string `json:"durable,omitempty"`
	// Workers is the number of messages handled at the same time. It matches
	// JetStream's default MaxAckPending of 1000 at most, since NATS would not
	// hand out more unacknowledged messages of a consumer anyway.
	Workers uint `json:"workers,omitempty" validate:"max=1000"`
}
