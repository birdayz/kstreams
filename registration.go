package kstreams

import (
	"github.com/birdayz/kstreams/internal/execution"
	"github.com/birdayz/kstreams/kdag"
	"github.com/birdayz/kstreams/kprocessor"
	"github.com/birdayz/kstreams/kserde"
)

// RegisterSource registers a source node that reads from a Kafka topic.
func RegisterSource[K, V any](b *kdag.Builder, name string, topic string, keyDeserializer kserde.Deserializer[K], valueDeserializer kserde.Deserializer[V]) error {
	return execution.RegisterSource(b, name, topic, keyDeserializer, valueDeserializer)
}

// MustRegisterSource is like RegisterSource but panics on error.
func MustRegisterSource[K, V any](b *kdag.Builder, name string, topic string, keyDeserializer kserde.Deserializer[K], valueDeserializer kserde.Deserializer[V]) {
	execution.MustRegisterSource(b, name, topic, keyDeserializer, valueDeserializer)
}

// RegisterProcessor registers a processor node.
// Deprecated: Use RegisterRecordProcessor for new code.
//
//nolint:staticcheck // SA1019: ProcessorBuilder kept for backward compatibility
func RegisterProcessor[Kin, Vin, Kout, Vout any](b *kdag.Builder, p kprocessor.ProcessorBuilder[Kin, Vin, Kout, Vout], name string, parent string, stores ...string) error {
	return execution.RegisterProcessor(b, p, name, parent, stores...)
}

// MustRegisterProcessor is like RegisterProcessor but panics on error.
// Deprecated: Use MustRegisterRecordProcessor for new code.
//
//nolint:staticcheck // SA1019: ProcessorBuilder kept for backward compatibility
func MustRegisterProcessor[Kin, Vin, Kout, Vout any](b *kdag.Builder, p kprocessor.ProcessorBuilder[Kin, Vin, Kout, Vout], name string, parent string, stores ...string) {
	execution.MustRegisterProcessor(b, p, name, parent, stores...)
}

// RegisterSink registers a sink node that writes to a Kafka topic.
func RegisterSink[K, V any](b *kdag.Builder, name, topic string, keySerializer kserde.Serializer[K], valueSerializer kserde.Serializer[V], parent string) error {
	return execution.RegisterSink(b, name, topic, keySerializer, valueSerializer, parent)
}

// MustRegisterSink is like RegisterSink but panics on error.
func MustRegisterSink[K, V any](b *kdag.Builder, name, topic string, keySerializer kserde.Serializer[K], valueSerializer kserde.Serializer[V], parent string) {
	execution.MustRegisterSink(b, name, topic, keySerializer, valueSerializer, parent)
}
