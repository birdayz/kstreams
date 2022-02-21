package streamz

type GenericProcessor[K any, V any] interface {
	Process(K, V) error
}

type Process0r[Kin any, Vin any, Kout any, Vout any] interface {
	Process(ctx Context[Kout, Vout], k Kin, v Vin) error
}
