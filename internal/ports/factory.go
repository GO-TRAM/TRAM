package ports

type Factory[S, T any] interface {
	New(S) (T, error)
}
