package internal

import (
	"context"

	"github.com/birdayz/kstreams/sdk"
	"github.com/hashicorp/go-multierror"
)

type GenericProcessor[K any, V any] interface {
	Process(context.Context, K, V) error
}

type Nexter[K, V any] interface {
	AddNext(GenericProcessor[K, V])
}

var _ = GenericProcessor[any, any](&ProcessorNode[any, any, any, any]{})

type ProcessorNode[Kin any, Vin any, Kout any, Vout any] struct {
	userProcessor sdk.Processor[Kin, Vin, Kout, Vout]
	outputs       map[string]GenericProcessor[Kout, Vout]
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

	var errs error
	for _, err := range userCtx.outputErrors {
		errs = multierror.Append(errs, err)
	}

	return errs
}

func (p *ProcessorNode[Kin, Vin, Kout, Vout]) Init(stores ...sdk.Store) error {
	return p.userProcessor.Init(stores...)
}

func (p *ProcessorNode[Kin, Vin, Kout, Vout]) Close() error {
	return nil
}
