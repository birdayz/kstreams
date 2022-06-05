package processors

import "github.com/birdayz/kstreams/sdk"

func ForEach[Kin, Vin any](forEachFunc func(k Kin, v Vin)) sdk.ProcessorBuilder[Kin, Vin, Kin, Vin] {
	return func() sdk.Processor[Kin, Vin, Kin, Vin] {
		return &ForEachProcessor[Kin, Vin]{
			forEachFunc: forEachFunc,
		}
	}
}

type ForEachProcessor[Kin, Vin any] struct {
	forEachFunc func(Kin, Vin)
}

func (p *ForEachProcessor[Kin, Vin]) Process(ctx sdk.Context[Kin, Vin], k Kin, v Vin) error {
	p.forEachFunc(k, v)
	return nil
}

func (p *ForEachProcessor[Kin, Vin]) Init(stores ...sdk.Store) error {
	return nil
}

func (p ForEachProcessor[Kin, Vin]) Close() error {
	return nil
}
