package execution

import (
	"fmt"
	"reflect"

	"github.com/birdayz/kstreams/internal/runtime"
	"github.com/birdayz/kstreams/kdag"
	"github.com/birdayz/kstreams/kprocessor"
	"github.com/birdayz/kstreams/kserde"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Registration functions for adding typed nodes to the DAG.
// These are in the execution package because they create runtime builders
// that implement SourceBuilder, ProcessorBuilder, and SinkBuilder interfaces.

// RegisterSource registers a source node that reads from a Kafka topic.
func RegisterSource[K, V any](b *kdag.Builder, name string, topic string, keyDeserializer kserde.Deserializer[K], valueDeserializer kserde.Deserializer[V]) error {
	nodeID := kdag.NodeID(name)

	if err := nodeID.Validate(); err != nil {
		return err
	}

	if _, exists := b.GetNode(nodeID); exists {
		return fmt.Errorf("%w: source %q", kdag.ErrNodeAlreadyExists, name)
	}

	var k K
	var v V

	builder := &sourceBuilder[K, V]{
		nodeID:       name,
		topic:        topic,
		keyDeser:     keyDeserializer,
		valueDeser:   valueDeserializer,
	}

	return b.AddSourceNode(name, topic, reflect.TypeOf(k), reflect.TypeOf(v), builder)
}

// MustRegisterSource is like RegisterSource but panics on error.
func MustRegisterSource[K, V any](b *kdag.Builder, name string, topic string, keyDeserializer kserde.Deserializer[K], valueDeserializer kserde.Deserializer[V]) {
	must(RegisterSource(b, name, topic, keyDeserializer, valueDeserializer))
}

// RegisterProcessor registers a processor node.
// Deprecated: Use RegisterRecordProcessor for new code.
//
//nolint:staticcheck // SA1019: ProcessorBuilder kept for backward compatibility
func RegisterProcessor[Kin, Vin, Kout, Vout any](b *kdag.Builder, p kprocessor.ProcessorBuilder[Kin, Vin, Kout, Vout], name string, parent string, stores ...string) error {
	nodeID := kdag.NodeID(name)
	parentID := kdag.NodeID(parent)

	if err := nodeID.Validate(); err != nil {
		return err
	}

	if _, exists := b.GetNode(nodeID); exists {
		return fmt.Errorf("%w: processor %q", kdag.ErrNodeAlreadyExists, name)
	}

	parentNode, ok := b.GetNode(parentID)
	if !ok {
		return fmt.Errorf("%w: parent node %q not found for processor %q (register parent first)",
			kdag.ErrNodeNotFound, parent, name)
	}

	// Validate stores exist
	graph := b.GetGraph()
	for _, storeName := range stores {
		if _, ok := graph.Stores[storeName]; !ok {
			return fmt.Errorf("%w: store %q referenced by processor %q (register store first)",
				kdag.ErrStoreNotFound, storeName, name)
		}
	}

	var kin Kin
	var vin Vin
	var kout Kout
	var vout Vout

	builder := &processorBuilder[Kin, Vin, Kout, Vout]{
		nodeID:        name,
		processorFunc: p,
		storeNames:    stores,
	}

	return b.AddProcessorNode(
		name,
		parent,
		reflect.TypeOf(kin),
		reflect.TypeOf(vin),
		reflect.TypeOf(kout),
		reflect.TypeOf(vout),
		stores,
		builder,
		parentNode,
	)
}

// MustRegisterProcessor is like RegisterProcessor but panics on error.
// Deprecated: Use MustRegisterRecordProcessor for new code.
//
//nolint:staticcheck // SA1019: ProcessorBuilder kept for backward compatibility
func MustRegisterProcessor[Kin, Vin, Kout, Vout any](b *kdag.Builder, p kprocessor.ProcessorBuilder[Kin, Vin, Kout, Vout], name string, parent string, stores ...string) {
	must(RegisterProcessor(b, p, name, parent, stores...))
}

// RegisterSink registers a sink node that writes to a Kafka topic.
func RegisterSink[K, V any](b *kdag.Builder, name, topic string, keySerializer kserde.Serializer[K], valueSerializer kserde.Serializer[V], parent string) error {
	nodeID := kdag.NodeID(name)
	parentID := kdag.NodeID(parent)

	if err := nodeID.Validate(); err != nil {
		return err
	}

	if _, exists := b.GetNode(nodeID); exists {
		return fmt.Errorf("%w: sink %q", kdag.ErrNodeAlreadyExists, name)
	}

	parentNode, ok := b.GetNode(parentID)
	if !ok {
		return fmt.Errorf("%w: parent node %q not found for sink %q (register parent first)",
			kdag.ErrNodeNotFound, parent, name)
	}

	var k K
	var v V

	builder := &sinkBuilder[K, V]{
		nodeID:    name,
		topic:     topic,
		keySer:    keySerializer,
		valueSer:  valueSerializer,
	}

	return b.AddSinkNode(
		name,
		topic,
		parent,
		reflect.TypeOf(k),
		reflect.TypeOf(v),
		builder,
		parentNode,
	)
}

// MustRegisterSink is like RegisterSink but panics on error.
func MustRegisterSink[K, V any](b *kdag.Builder, name, topic string, keySerializer kserde.Serializer[K], valueSerializer kserde.Serializer[V], parent string) {
	must(RegisterSink(b, name, topic, keySerializer, valueSerializer, parent))
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

// sourceBuilder implements SourceBuilder for source nodes.
type sourceBuilder[K, V any] struct {
	nodeID     string
	topic      string
	keyDeser   kserde.Deserializer[K]
	valueDeser kserde.Deserializer[V]
}

// BuilderKind implements kdag.RuntimeBuilder.
func (s *sourceBuilder[K, V]) BuilderKind() kdag.NodeType {
	return kdag.NodeTypeSource
}

func (s *sourceBuilder[K, V]) Build(children map[string]RuntimeNode) (RuntimeNode, error) {
	sourceNode := runtime.NewRuntimeSourceNode(s.nodeID, s.topic, s.keyDeser, s.valueDeser)

	for _, child := range children {
		if processor, ok := child.(runtime.InputProcessor[K, V]); ok {
			sourceNode.AddDownstream(processor)
		} else {
			return nil, fmt.Errorf("child does not implement InputProcessor[%T, %T]", *new(K), *new(V))
		}
	}

	return sourceNode, nil
}

// processorBuilder implements ProcessorBuilder for processor nodes.
type processorBuilder[Kin, Vin, Kout, Vout any] struct {
	nodeID        string
	processorFunc kprocessor.ProcessorBuilder[Kin, Vin, Kout, Vout] //nolint:staticcheck // SA1019: backward compat
	storeNames    []string
}

// BuilderKind implements kdag.RuntimeBuilder.
func (p *processorBuilder[Kin, Vin, Kout, Vout]) BuilderKind() kdag.NodeType {
	return kdag.NodeTypeProcessor
}

func (p *processorBuilder[Kin, Vin, Kout, Vout]) Build(
	stores map[string]kprocessor.Store,
	collector runtime.ChangelogCollector,
	sm runtime.StateManager,
	children map[string]RuntimeNode,
) (RuntimeNode, error) {
	// Build processor stores
	processorStores := make(map[string]kprocessor.Store, len(p.storeNames))
	for _, storeName := range p.storeNames {
		if store, ok := stores[storeName]; ok {
			processorStores[storeName] = store
		} else {
			return nil, fmt.Errorf("store %q not found for processor %s", storeName, p.nodeID)
		}
	}

	// Create processor context with children as outputs
	outputs := make(map[string]runtime.InputProcessor[Kout, Vout], len(children))
	for name, child := range children {
		if processor, ok := child.(runtime.InputProcessor[Kout, Vout]); ok {
			outputs[name] = processor
		} else {
			return nil, fmt.Errorf("child %s does not implement InputProcessor[%T, %T]", name, *new(Kout), *new(Vout))
		}
	}

	ctx := runtime.NewIsolatedProcessorContext[Kout, Vout](
		runtime.NodeID(p.nodeID),
		processorStores,
		sm,
		collector,
	)

	// Set outputs on context
	for name, output := range outputs {
		ctx.AddOutput(runtime.NodeID(name), output)
	}

	// Create processor node
	node := runtime.NewRuntimeProcessorNode[Kin, Vin, Kout, Vout](
		p.nodeID,
		p.processorFunc(),
		ctx,
	)

	return node, nil
}

// sinkBuilder implements SinkBuilder for sink nodes.
type sinkBuilder[K, V any] struct {
	nodeID   string
	topic    string
	keySer   kserde.Serializer[K]
	valueSer kserde.Serializer[V]
}

// BuilderKind implements kdag.RuntimeBuilder.
func (s *sinkBuilder[K, V]) BuilderKind() kdag.NodeType {
	return kdag.NodeTypeSink
}

func (s *sinkBuilder[K, V]) Build(client *kgo.Client) (RuntimeNode, error) {
	return runtime.NewRuntimeSinkNode(
		s.nodeID,
		client,
		s.topic,
		s.keySer,
		s.valueSer,
	), nil
}

// Compile-time interface checks
var (
	_ kdag.RuntimeBuilder = (*sourceBuilder[string, string])(nil)
	_ kdag.RuntimeBuilder = (*processorBuilder[string, string, string, string])(nil)
	_ kdag.RuntimeBuilder = (*sinkBuilder[string, string])(nil)
)
