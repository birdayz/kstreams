package processors

import (
	"context"

	"github.com/birdayz/kstreams"
)

func ForEach[Kin, Vin any](forEachFunc func(k Kin, v Vin)) kstreams.ProcessorBuilder[Kin, Vin, Kin, Vin] {
	return func() kstreams.Processor[Kin, Vin, Kin, Vin] {
		return &ForEachProcessor[Kin, Vin]{
			forEachFunc: forEachFunc,
		}
	}
}

type ForEachProcessor[Kin, Vin any] struct {
	forEachFunc func(Kin, Vin)
}

func (p *ForEachProcessor[Kin, Vin]) Process(ctx context.Context, k Kin, v Vin) error {
	p.forEachFunc(k, v)
	return nil
}

func (p *ForEachProcessor[Kin, Vin]) Init(processorContext kstreams.ProcessorContext[Kin, Vin]) error {
	return nil
}

func (p ForEachProcessor[Kin, Vin]) Close() error {
	return nil
}
