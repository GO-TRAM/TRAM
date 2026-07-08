package generator

type Spec struct {
	Interval string `json:"interval" validate:"required"`
}
