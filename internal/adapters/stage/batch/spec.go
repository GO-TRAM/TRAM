package batch

type Spec struct {
	MaxRecords int    `json:"max_records,omitempty" validate:"omitempty,min=1"`
	Timeout    string `json:"timeout,omitempty"`
}
