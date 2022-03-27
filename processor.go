package streamz

import "github.com/birdayz/streamz/internal"

func RegisterProcessor[Kin, Vin, Kout, Vout any](t *internal.TopologyBuilder, p internal.ProcessorBuilder[Kin, Vin, Kout, Vout]) {
	internal.MustAddProcessor(t, p)
}

func RegisterProcessorFunc[Kin, Vin, Kout, Vout any](t *internal.TopologyBuilder, fn func(ctx internal.Context[Kout, Vout], k Kin, v Vin) error) {
	bldr := internal.ProcessorBuilder[Kin, Vin, Kout, Vout](&GenericProcessorBuilder[Kin, Vin, Kout, Vout]{
		name:        "",
		processFunc: fn,
	})
	internal.MustAddProcessor(t, bldr)
}

type GenericProcessorBuilder[Kin, Vin, Kout, Vout any] struct {
	name        string
	processFunc func(ctx internal.Context[Kout, Vout], k Kin, v Vin) error
}

type GenericProcessor[Kin, Vin, Kout, Vout any] struct {
	processFunc func(ctx internal.Context[Kout, Vout], k Kin, v Vin) error
}

func (g *GenericProcessor[Kin, Vin, Kout, Vout]) Process(ctx internal.Context[Kout, Vout], k Kin, v Vin) error {
	return g.processFunc(ctx, k, v)
}

func (g *GenericProcessorBuilder[Kin, Vin, Kout, Vout]) Name() string {
	return g.name
}

func (g *GenericProcessorBuilder[Kin, Vin, Kout, Vout]) Build() internal.Processor[Kin, Vin, Kout, Vout] {
	return &GenericProcessor[Kin, Vin, Kout, Vout]{
		processFunc: g.processFunc,
	}
}
