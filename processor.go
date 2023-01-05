package kstreams

import "context"

// Processor is a low-level interface. The implementation can retain the
// ProcessorContext passed into Init and use it to access state stores and
// forward data to downstream nodes. This is fairly low-level and allows for a
// lot of flexibility, but may be inconvenient to use for more specialized use
// cases. More high-level interfaces can be built on top of this, i.e. a
// Processor that receives input, and forwards it to only one downstream node.
type Processor[Kin any, Vin any, Kout any, Vout any] interface {
	Init(ProcessorContext[Kout, Vout]) error
	Close() error
	Process(ctx context.Context, k Kin, v Vin) error
}

// ProcessorBuilder creates an actual processor for a specific TopicPartition.
type ProcessorBuilder[Kin any, Vin any, Kout any, Vout any] func() Processor[Kin, Vin, Kout, Vout]
