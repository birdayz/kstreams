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

func RegisterProcessor[Kin, Vin, Kout, Vout any](t *TopologyBuilder, p sdk.ProcessorBuilder[Kin, Vin, Kout, Vout], name, parent string) {
	internal.MustAddProcessor(t, p, name)
	internal.MustSetParent(t, parent, name)
}
