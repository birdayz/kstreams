package streamz

import (
	"github.com/birdayz/streamz/internal"
	"github.com/birdayz/streamz/sdk"
)

type TopologyBuilder = internal.TopologyBuilder

func NewTopologyBuilder() *TopologyBuilder {
	return internal.NewTopologyBuilder()
}

func RegisterSource[K, V any](t *TopologyBuilder, name string, topic string, keyDeserializer sdk.Deserializer[K], valueDeserializer sdk.Deserializer[V]) {
	internal.MustAddSource(t, name, topic, keyDeserializer, valueDeserializer)
}

func RegisterProcessor[Kin, Vin, Kout, Vout any](t *TopologyBuilder, p sdk.ProcessorBuilder[Kin, Vin, Kout, Vout], parent string) {
	internal.MustAddProcessor(t, p)
	internal.MustSetParent(t, parent, p.Name())
}

func RegisterProcessorFunc[Kin, Vin, Kout, Vout any](t *TopologyBuilder, fn func(ctx sdk.Context[Kout, Vout], k Kin, v Vin) error, name, parent string) {
	bldr := sdk.ProcessorBuilder[Kin, Vin, Kout, Vout](&GenericProcessorBuilder[Kin, Vin, Kout, Vout]{
		name:        name,
		processFunc: fn,
	})

	internal.MustAddProcessor(t, sdk.ProcessorBuilder[Kin, Vin, Kout, Vout](bldr))
	internal.MustSetParent(t, parent, name)
}

type GenericProcessorBuilder[Kin, Vin, Kout, Vout any] struct {
	name        string
	processFunc func(ctx sdk.Context[Kout, Vout], k Kin, v Vin) error
}

type GenericProcessor[Kin, Vin, Kout, Vout any] struct {
	processFunc func(ctx sdk.Context[Kout, Vout], k Kin, v Vin) error
}

func (g *GenericProcessor[Kin, Vin, Kout, Vout]) Process(ctx sdk.Context[Kout, Vout], k Kin, v Vin) error {
	return g.processFunc(ctx, k, v)
}

func (g *GenericProcessorBuilder[Kin, Vin, Kout, Vout]) Name() string {
	return g.name
}

func (g *GenericProcessorBuilder[Kin, Vin, Kout, Vout]) Build() sdk.Processor[Kin, Vin, Kout, Vout] {
	return &GenericProcessor[Kin, Vin, Kout, Vout]{
		processFunc: g.processFunc,
	}
}

type SimpleProcessorBuilder[Kin any, Vin any, Kout any, Vout any] struct {
	name      string
	buildFunc func() sdk.Processor[Kin, Vin, Kout, Vout]
}

func (s *SimpleProcessorBuilder[Kin, Vin, Kout, Vout]) Build() sdk.Processor[Kin, Vin, Kout, Vout] {
	return s.buildFunc()
}

func (s *SimpleProcessorBuilder[Kin, Vin, Kout, Vout]) Name() string {
	return s.name
}

func NewProcessorBuilder[Kin any, Vin any, Kout any, Vout any](name string, buildFunc func() sdk.Processor[Kin, Vin, Kout, Vout]) sdk.ProcessorBuilder[Kin, Vin, Kout, Vout] {
	return &SimpleProcessorBuilder[Kin, Vin, Kout, Vout]{
		name:      name,
		buildFunc: buildFunc,
	}
}
