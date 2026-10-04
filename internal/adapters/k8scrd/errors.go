package k8scrd

import "errors"

var (
	ErrEmptyNamespace        = errors.New("empty namespace in watch namespaces")
	ErrInvalidStreamSelector = errors.New("invalid stream selector")
)
