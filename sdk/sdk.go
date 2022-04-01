package sdk

type Context[Kout any, Vout any] interface {
	Forward(k Kout, v Vout)
}

type Processor[Kin any, Vin any, Kout any, Vout any] interface {
	Process(ctx Context[Kout, Vout], k Kin, v Vin) error
}

type ProcessorBuilder[Kin any, Vin any, Kout any, Vout any] interface {
	Name() string
	Build() Processor[Kin, Vin, Kout, Vout]
}

type Serializer[T any] func(T) ([]byte, error)

type Deserializer[T any] func([]byte) (T, error)

type SerDe[T any] struct {
	Serializer   Serializer[T]
	Deserializer Deserializer[T]
}
