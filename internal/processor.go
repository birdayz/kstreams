package internal

type GenericProcessor[K any, V any] interface {
	Process(K, V) error
}

type Processor[Kin any, Vin any, Kout any, Vout any] interface {
	Process(ctx Context[Kout, Vout], k Kin, v Vin) error
}
