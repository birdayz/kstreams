package kstreams

import (
	"context"

	"github.com/hashicorp/go-multierror"
)

// InputProcessor is a partial interface covering only the generic input K/V,
// without requiring the caller to know the generic types of the output.
type InputProcessor[K any, V any] interface {
	Process(context.Context, K, V) error
}

var _ = InputProcessor[any, any](&ProcessorNode[any, any, any, any]{})

type ProcessorNode[Kin any, Vin any, Kout any, Vout any] struct {
	userProcessor Processor[Kin, Vin, Kout, Vout]
	outputs       map[string]InputProcessor[Kout, Vout]
}

func (p *ProcessorNode[Kin, Vin, Kout, Vout]) Process(ctx context.Context, k Kin, v Vin) error {
	userCtx := ProcessorContext[Kout, Vout]{
		Context: ctx,
		outputs: p.outputs,
	}

	err := p.userProcessor.Process(&userCtx, k, v)
	if err != nil {
		return err
	}

	var errs *multierror.Error
	for _, err := range userCtx.outputErrors {
		errs = multierror.Append(errs, err)
	}

	return errs.ErrorOrNil()
}

func (p *ProcessorNode[Kin, Vin, Kout, Vout]) Init(stores ...Store) error {
	return p.userProcessor.Init(stores...)
}

func (p *ProcessorNode[Kin, Vin, Kout, Vout]) Close() error {
	return nil
}
