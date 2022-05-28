package kstreams

import (
	"github.com/birdayz/kstreams/internal"
	"github.com/birdayz/kstreams/sdk"
)

type Topology = internal.TopologyBuilder

func NewTopology() *Topology {
	return internal.NewTopologyBuilder()
}

func RegisterSource[K, V any](t *Topology, name string, topic string, keyDeserializer sdk.Deserializer[K], valueDeserializer sdk.Deserializer[V]) {
	internal.MustAddSource(t, name, topic, keyDeserializer, valueDeserializer)
}

func RegisterSink[K, V any](t *Topology, name string, topic string, keySerializer sdk.Serializer[K], valueSerializer sdk.Serializer[V], parent string) {
	internal.MustAddSink(t, name, topic, keySerializer, valueSerializer)
	internal.MustSetParent(t, parent, name)
}

func RegisterProcessor[Kin, Vin, Kout, Vout any](t *Topology, p sdk.ProcessorBuilder[Kin, Vin, Kout, Vout], name, parent string, stores ...string) {
	internal.MustAddProcessor(t, p, name, stores...)
	internal.MustSetParent(t, parent, name)
}
