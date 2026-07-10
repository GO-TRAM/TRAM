package celvalidator

type Spec struct {
	Logs       string `json:"logs"`
	Metrics    string `json:"metrics"`
	Traces     string `json:"traces"`
	OnMismatch string `json:"on_mismatch" validate:"required,oneof=drop reject"`
}
