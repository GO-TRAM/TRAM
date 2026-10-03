package controller

import (
	tramv1 "codeberg.org/eljoth/tram/internal/adapters/k8scrd/api/v1"
	"codeberg.org/eljoth/tram/internal/core/domain"
)

func ToDomain(name string, s tramv1.Stream) (*domain.StreamSpec, error) {
	return toDomain(name, s)
}
