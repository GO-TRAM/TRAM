package celfilter

type Spec struct {
	Expression string `json:"expression" validate:"required"`
	OnMismatch string `json:"on_mismatch" validate:"required,oneof=drop reject"`
}
