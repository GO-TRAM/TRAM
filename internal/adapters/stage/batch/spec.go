package batch

type Spec struct {
	// MaxRecords flushes the batch once it holds at least this many records
	// (log records, metric data points and spans). A single event is never
	// split, so a batch can exceed it by the size of the last event.
	MaxRecords int `json:"max_records,omitempty" validate:"omitempty,min=1"`
	// Timeout flushes a batch this long after its first event arrived.
	Timeout string `json:"timeout,omitempty"`
}
