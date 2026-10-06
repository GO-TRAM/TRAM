package celfilter

type Spec struct {
	Logs    string `json:"logs,omitempty"    validate:"required_without_all=Metrics Traces"`
	Metrics string `json:"metrics,omitempty" validate:"required_without_all=Logs Traces"`
	Traces  string `json:"traces,omitempty"  validate:"required_without_all=Logs Metrics"`
}
