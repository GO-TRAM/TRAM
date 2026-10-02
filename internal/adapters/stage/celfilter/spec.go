package celfilter

// Spec holds one CEL expression per signal type, evaluated per record. A
// record is kept if its expression returns true. Signal types without an
// expression are passed on unfiltered; at least one expression is required.
type Spec struct {
	Logs    string `json:"logs,omitempty" validate:"required_without_all=Metrics Traces"`
	Metrics string `json:"metrics,omitempty" validate:"required_without_all=Logs Traces"`
	Traces  string `json:"traces,omitempty" validate:"required_without_all=Logs Metrics"`
}
