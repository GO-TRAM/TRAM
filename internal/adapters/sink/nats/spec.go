package nats

type Spec struct {
	URL       string      `json:"url" validate:"required"`
	Stream    string      `json:"stream" validate:"required"`
	Subject   SubjectSpec `json:"subject" validate:"required"`
	Retention string      `json:"retention"`
	Size      int64       `json:"size"`
}

type SubjectSpec struct {
	Register []string `json:"register" validate:"required,min=1"`
	Logs     string   `json:"logs,omitempty" validate:"required,min=1"`
	Metrics  string   `json:"metrics,omitempty" validate:"required,min=1"`
	Traces   string   `json:"traces,omitempty" validate:"required,min=1"`
}
