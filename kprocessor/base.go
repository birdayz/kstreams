package kprocessor

import "context"

// FuncOption configures optional behavior for NewFunc processors.
type FuncOption[Kin, Vin, Kout, Vout any] func(*funcProcessor[Kin, Vin, Kout, Vout])

// WithInit adds custom initialization logic to a NewFunc processor.
func WithInit[Kin, Vin, Kout, Vout any](fn func(ctx ProcessorContext[Kout, Vout]) error) FuncOption[Kin, Vin, Kout, Vout] {
	return func(p *funcProcessor[Kin, Vin, Kout, Vout]) {
		p.initFn = fn
	}
}

// WithClose adds custom cleanup logic to a NewFunc processor.
func WithClose[Kin, Vin, Kout, Vout any](fn func() error) FuncOption[Kin, Vin, Kout, Vout] {
	return func(p *funcProcessor[Kin, Vin, Kout, Vout]) {
		p.closeFn = fn
	}
}

// NewFunc creates a ProcessorBuilder from a function.
// The process function receives the processor context for forwarding and store access.
//
// Deprecated: Use NewRecordFunc instead for access to record metadata (headers,
// timestamps, offsets). NewFunc creates a legacy Processor that receives only
// bare key-value pairs.
//
// Example:
//
//	kprocessor.NewFunc(func(pctx kprocessor.ProcessorContext[string, int], ctx context.Context, k, v string) error {
//	    pctx.Forward(ctx, k, len(v))
//	    return nil
//	})
//
// With init/close:
//
//	kprocessor.NewFunc(processFn,
//	    kprocessor.WithInit(func(ctx kprocessor.ProcessorContext[K, V]) error { ... }),
//	    kprocessor.WithClose(func() error { ... }),
//	)
func NewFunc[Kin, Vin, Kout, Vout any](
	processFn func(ctx ProcessorContext[Kout, Vout], c context.Context, k Kin, v Vin) error,
	opts ...FuncOption[Kin, Vin, Kout, Vout],
) ProcessorBuilder[Kin, Vin, Kout, Vout] {
	return func() Processor[Kin, Vin, Kout, Vout] {
		p := &funcProcessor[Kin, Vin, Kout, Vout]{
			processFn: processFn,
		}
		for _, opt := range opts {
			opt(p)
		}
		return p
	}
}

// funcProcessor is the internal processor implementation for NewFunc.
type funcProcessor[Kin, Vin, Kout, Vout any] struct {
	ctx       ProcessorContext[Kout, Vout]
	processFn func(ProcessorContext[Kout, Vout], context.Context, Kin, Vin) error
	initFn    func(ProcessorContext[Kout, Vout]) error
	closeFn   func() error
}

func (p *funcProcessor[Kin, Vin, Kout, Vout]) Init(ctx ProcessorContext[Kout, Vout]) error {
	p.ctx = ctx
	if p.initFn != nil {
		return p.initFn(ctx)
	}
	return nil
}

func (p *funcProcessor[Kin, Vin, Kout, Vout]) Close() error {
	if p.closeFn != nil {
		return p.closeFn()
	}
	return nil
}

func (p *funcProcessor[Kin, Vin, Kout, Vout]) Process(ctx context.Context, k Kin, v Vin) error {
	return p.processFn(p.ctx, ctx, k, v)
}

// RecordFuncOption configures optional behavior for NewRecordFunc processors.
type RecordFuncOption[Kin, Vin, Kout, Vout any] func(*recordFuncProcessor[Kin, Vin, Kout, Vout])

// WithRecordInit adds custom initialization logic to a NewRecordFunc processor.
func WithRecordInit[Kin, Vin, Kout, Vout any](fn func(ctx RecordProcessorContext[Kout, Vout]) error) RecordFuncOption[Kin, Vin, Kout, Vout] {
	return func(p *recordFuncProcessor[Kin, Vin, Kout, Vout]) {
		p.initFn = fn
	}
}

// WithRecordClose adds custom cleanup logic to a NewRecordFunc processor.
func WithRecordClose[Kin, Vin, Kout, Vout any](fn func() error) RecordFuncOption[Kin, Vin, Kout, Vout] {
	return func(p *recordFuncProcessor[Kin, Vin, Kout, Vout]) {
		p.closeFn = fn
	}
}

// NewRecordFunc creates a RecordProcessorBuilder from a function.
// The process function receives full Record objects with metadata (headers, timestamps, offsets).
//
// Example:
//
//	kprocessor.NewRecordFunc(func(pctx kprocessor.RecordProcessorContext[string, int], ctx context.Context, record kprocessor.Record[string, string]) error {
//	    // Access headers, timestamp, offset, etc.
//	    if traceID, ok := record.Metadata.Headers.GetString("trace-id"); ok {
//	        log.Info("processing", "trace_id", traceID)
//	    }
//	    pctx.Forward(ctx, record.Key, len(record.Value))
//	    return nil
//	})
func NewRecordFunc[Kin, Vin, Kout, Vout any](
	processFn func(ctx RecordProcessorContext[Kout, Vout], c context.Context, record Record[Kin, Vin]) error,
	opts ...RecordFuncOption[Kin, Vin, Kout, Vout],
) RecordProcessorBuilder[Kin, Vin, Kout, Vout] {
	return func() RecordProcessor[Kin, Vin, Kout, Vout] {
		p := &recordFuncProcessor[Kin, Vin, Kout, Vout]{
			processFn: processFn,
		}
		for _, opt := range opts {
			opt(p)
		}
		return p
	}
}

// recordFuncProcessor is the internal processor implementation for NewRecordFunc.
type recordFuncProcessor[Kin, Vin, Kout, Vout any] struct {
	ctx       RecordProcessorContext[Kout, Vout]
	processFn func(RecordProcessorContext[Kout, Vout], context.Context, Record[Kin, Vin]) error
	initFn    func(RecordProcessorContext[Kout, Vout]) error
	closeFn   func() error
}

func (p *recordFuncProcessor[Kin, Vin, Kout, Vout]) Init(ctx RecordProcessorContext[Kout, Vout]) error {
	p.ctx = ctx
	if p.initFn != nil {
		return p.initFn(ctx)
	}
	return nil
}

func (p *recordFuncProcessor[Kin, Vin, Kout, Vout]) Close() error {
	if p.closeFn != nil {
		return p.closeFn()
	}
	return nil
}

func (p *recordFuncProcessor[Kin, Vin, Kout, Vout]) ProcessRecord(ctx context.Context, record Record[Kin, Vin]) error {
	return p.processFn(p.ctx, ctx, record)
}
