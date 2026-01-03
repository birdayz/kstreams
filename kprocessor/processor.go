package kprocessor

import (
	"context"
)

// Processor is a low-level interface. The implementation can retain the
// ProcessorContext passed into Init and use it to access state stores and
// forward data to downstream nodes. This is fairly low-level and allows for a
// lot of flexibility, but may be inconvenient to use for more specialized use
// cases. More high-level interfaces can be built on top of this, i.e. a
// Processor that receives input, and forwards it to only one downstream node.
//
// Deprecated: Use RecordProcessor instead. Processor receives only bare key-value
// pairs without metadata (headers, timestamps, offsets). RecordProcessor provides
// full record metadata access and should be used for all new code.
type Processor[Kin any, Vin any, Kout any, Vout any] interface {
	Init(ProcessorContext[Kout, Vout]) error
	Close() error
	Process(ctx context.Context, k Kin, v Vin) error
}

// RecordProcessor is an enhanced processor interface that receives full Record objects
// with metadata (headers, timestamps, offsets, etc.) instead of just bare key-value pairs.
//
// This interface provides access to:
// - Record metadata (topic, partition, offset, timestamp, leader epoch)
// - Headers for tracing, correlation IDs, etc.
// - Enhanced context with punctuation, stream time, and record forwarding
//
// Use RecordProcessor for new code that needs access to record metadata.
// For simple key-value processing without metadata, the legacy Processor interface can still be used.
type RecordProcessor[Kin any, Vin any, Kout any, Vout any] interface {
	Init(RecordProcessorContext[Kout, Vout]) error
	Close() error
	ProcessRecord(ctx context.Context, record Record[Kin, Vin]) error
}

// ProcessorBuilder creates an actual processor for a specific TopicPartition.
//
// Deprecated: Use RecordProcessorBuilder instead. ProcessorBuilder creates
// legacy Processor instances that lack access to record metadata.
type ProcessorBuilder[Kin any, Vin any, Kout any, Vout any] func() Processor[Kin, Vin, Kout, Vout]

// RecordProcessorBuilder creates a RecordProcessor for a specific TopicPartition.
// This is the preferred builder type for new code.
type RecordProcessorBuilder[Kin any, Vin any, Kout any, Vout any] func() RecordProcessor[Kin, Vin, Kout, Vout]

// ProcessorWithInterceptors wraps a RecordProcessor with interceptors
type ProcessorWithInterceptors[Kin, Vin, Kout, Vout any] struct {
	processor    RecordProcessor[Kin, Vin, Kout, Vout]
	interceptors *InterceptorChain[Kin, Vin]
}

func (p *ProcessorWithInterceptors[Kin, Vin, Kout, Vout]) Init(ctx RecordProcessorContext[Kout, Vout]) error {
	return p.processor.Init(ctx)
}

func (p *ProcessorWithInterceptors[Kin, Vin, Kout, Vout]) Close() error {
	return p.processor.Close()
}

func (p *ProcessorWithInterceptors[Kin, Vin, Kout, Vout]) ProcessRecord(
	ctx context.Context,
	record Record[Kin, Vin],
) error {
	// Execute interceptor chain
	return p.interceptors.Execute(ctx, record, func(ctx context.Context, rec Record[Kin, Vin]) error {
		return p.processor.ProcessRecord(ctx, rec)
	})
}

// WithInterceptors wraps a processor with interceptors
func WithInterceptors[Kin, Vin, Kout, Vout any](
	processor RecordProcessor[Kin, Vin, Kout, Vout],
	interceptors ...ProcessorInterceptor[Kin, Vin],
) RecordProcessor[Kin, Vin, Kout, Vout] {
	return &ProcessorWithInterceptors[Kin, Vin, Kout, Vout]{
		processor:    processor,
		interceptors: ChainInterceptors(interceptors...),
	}
}
