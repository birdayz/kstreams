package streamz

type Serializer[T any] func(T) ([]byte, error)

type Deserializer[T any] func([]byte) (T, error)

type SerDe[T any] struct {
	Serializer   Serializer[T]
	Deserializer Deserializer[T]
}
