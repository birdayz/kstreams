package internal

import (
	"context"

	"github.com/birdayz/kstreams/sdk"
)

type Nexter[K, V any] interface {
	AddNext(GenericProcessor[K, V])
}

var _ = GenericProcessor[any, any](&Process0rNode[any, any, any, any]{})

type Process0rNode[Kin any, Vin any, Kout any, Vout any] struct {
	userProcessor sdk.Processor[Kin, Vin, Kout, Vout]
	outputs       map[string]GenericProcessor[Kout, Vout]

	ctx *ProcessorContext[Kout, Vout]
}

func (p *Process0rNode[Kin, Vin, Kout, Vout]) Process(ctx context.Context, k Kin, v Vin) error {
	// FIXME create ctx here, not anywhere else (topo)
	// set ctx in..ctx
	err := p.userProcessor.Process(p.ctx, k, v)
	if err != nil {
		return err
	}

	var firstError error
	for _, err := range p.ctx.outputErrors {
		firstError = err
	}
	if firstError != nil {
		p.ctx.outputErrors = map[string]error{}
	}

	return firstError
}

func (p *Process0rNode[Kin, Vin, Kout, Vout]) Init(stores ...sdk.Store) error {
	return p.userProcessor.Init(stores...)
}

func (p *Process0rNode[Kin, Vin, Kout, Vout]) Close() error {
	return nil
}
