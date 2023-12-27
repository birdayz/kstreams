package kstreams

import (
	"context"
	"errors"
)

// InputProcessor is a partial interface covering only the generic input K/V,
// without requiring the caller to know the generic types of the output.
type InputProcessor[K any, V any] interface {
	Process(context.Context, K, V) error
}

var _ = InputProcessor[any, any](&ProcessorNode[any, any, any, any]{})

type ProcessorNode[Kin any, Vin any, Kout any, Vout any] struct {
	userProcessor    Processor[Kin, Vin, Kout, Vout]
	processorContext *InternalProcessorContext[Kout, Vout]
}

func (p *ProcessorNode[Kin, Vin, Kout, Vout]) Process(ctx context.Context, k Kin, v Vin) error {
	err := p.userProcessor.Process(ctx, k, v)
	if err != nil {
		return err
	}

	// FIXME this does not work. every node writes to the ctx and it's more or less random which node gets which error...
	errz := p.processorContext.drainErrors()
	if len(errz) > 0 {
		var errs error
		for _, err := range errz {
			errs = errors.Join(errs, err)
		}
		return errs
	}

	return nil
}

func (p *ProcessorNode[Kin, Vin, Kout, Vout]) Init() error {
	return p.userProcessor.Init(p.processorContext)
}

func (p *ProcessorNode[Kin, Vin, Kout, Vout]) Close() error {
	return nil
}
