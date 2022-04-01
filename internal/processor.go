package internal

type GenericProcessor[K any, V any] interface {
	Process(K, V) error
}
