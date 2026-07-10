package celvalidator

type Spec struct {
	LogsFilter    string `json:"logs"`
	MetricsFilter string `json:"metrics"`
	TracesFilter  string `json:"traces"`
	OnMismatch    string `json:"on_mismatch" validate:"required,oneof=drop reject"`
}
