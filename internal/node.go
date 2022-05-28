package internal

import (
	"context"

	"github.com/birdayz/kstreams/sdk"
	"go.uber.org/multierr"
)

type Nexter[K, V any] interface {
	AddNext(GenericProcessor[K, V])
}

var _ = GenericProcessor[any, any](&Processor[any, any, any, any]{})

type Processor[Kin any, Vin any, Kout any, Vout any] struct {
	userProcessor sdk.Processor[Kin, Vin, Kout, Vout]
	outputs       map[string]GenericProcessor[Kout, Vout]
}

func (p *Processor[Kin, Vin, Kout, Vout]) Process(ctx context.Context, k Kin, v Vin) error {
	userCtx := ProcessorContext[Kout, Vout]{
		ctx:     ctx,
		outputs: p.outputs,
	}

	err := p.userProcessor.Process(&userCtx, k, v)
	if err != nil {
		return err
	}

	var errs error
	for _, err := range userCtx.outputErrors {
		multierr.AppendInto(&errs, err)
	}

	return errs
}

func (p *Processor[Kin, Vin, Kout, Vout]) Init(stores ...sdk.Store) error {
	return p.userProcessor.Init(stores...)
}

func (p *Processor[Kin, Vin, Kout, Vout]) Close() error {
	return nil
}
