package domain

type Stage interface {
	Emitter
	Registry
}

type StageSpec struct {
	Name string
	Type string
	From string
}
