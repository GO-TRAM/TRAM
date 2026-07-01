package domain

import "context"

type Source interface {
	Emitter
	Registry

	Run(ctx context.Context) error
	Stop() error
}

type SourceSpec struct {
	Name string
	Type string
}
