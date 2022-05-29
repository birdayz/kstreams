package sdk

import "context"

type Context[Kout any, Vout any] interface {
	context.Context
	Forward(k Kout, v Vout, processors ...string)
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
