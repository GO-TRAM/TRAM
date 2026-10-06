package nats

type Spec struct {
	URL         string      `json:"url"         validate:"required"`
	Stream      string      `json:"stream"      validate:"required"`
	Subject     SubjectSpec `json:"subject"     validate:"required"`
	Retention   string      `json:"retention"`
	MaxAge      string      `json:"max_age"`
	Size        int64       `json:"size"`
	Compression string      `json:"compression" validate:"omitempty,oneof=none s2"`
}

type SubjectSpec struct {
	Register []string `json:"register"          validate:"required,min=1"`
	Logs     string   `json:"logs,omitempty"    validate:"required_without_all=Metrics Traces"`
	Metrics  string   `json:"metrics,omitempty" validate:"required_without_all=Logs Traces"`
	Traces   string   `json:"traces,omitempty"  validate:"required_without_all=Logs Metrics"`
}
