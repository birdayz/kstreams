package sdk

type Serializer[T any] func(T) ([]byte, error)

type Deserializer[T any] func([]byte) (T, error)
