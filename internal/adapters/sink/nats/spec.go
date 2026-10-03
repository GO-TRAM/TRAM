package nats

type Spec struct {
	URL       string      `json:"url"       validate:"required"`
	Stream    string      `json:"stream"    validate:"required"`
	Subject   SubjectSpec `json:"subject"   validate:"required"`
	Retention string      `json:"retention"`
	Size      int64       `json:"size"`
}

// SubjectSpec maps each signal type to a CEL expression that computes the
// subject. Signals without an expression are not published; at least one
// expression is required.
type SubjectSpec struct {
	Register []string `json:"register"          validate:"required,min=1"`
	Logs     string   `json:"logs,omitempty"    validate:"required_without_all=Metrics Traces"`
	Metrics  string   `json:"metrics,omitempty" validate:"required_without_all=Logs Traces"`
	Traces   string   `json:"traces,omitempty"  validate:"required_without_all=Logs Metrics"`
}
