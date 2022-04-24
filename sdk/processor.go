package sdk

type Context[Kout any, Vout any] interface {
	Forward(k Kout, v Vout)
}

type BaseProcessor interface {
	Init(stores ...Store) error
	Close() error
}

type Processor[Kin any, Vin any, Kout any, Vout any] interface {
	BaseProcessor
	Process(ctx Context[Kout, Vout], k Kin, v Vin) error
}

type ProcessorBuilder[Kin any, Vin any, Kout any, Vout any] func() Processor[Kin, Vin, Kout, Vout]
