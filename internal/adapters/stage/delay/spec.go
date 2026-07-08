package delay

type Spec struct {
	Delay string `json:"delay" validate:"required"`
}
