package kstreams

import (
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

type TopologyBuilder struct {
	// Use new TopologyGraph internally
	graph *TopologyGraph

	// OLD architecture - being removed
	processors map[string]*TopologyProcessor
	// stores     map[string]*TopologyStore // REMOVED
	sources    map[string]*TopologySource
	sinks      map[string]*TopologySink
}

func (tb *TopologyBuilder) Build() (*Topology, error) {
	// Validate the graph (includes cycle detection!)
	if err := tb.graph.Validate(); err != nil {
		return nil, fmt.Errorf("validation failed: %w", err)
	}

	// Compute partition groups deterministically
	partitionGroups, err := tb.graph.ComputePartitionGroups()
	if err != nil {
		return nil, fmt.Errorf("partition groups: %w", err)
	}

	return &Topology{
		graph:           tb.graph,
		partitionGroups: partitionGroups,
		// OLD architecture
		sources:    tb.sources,
		// stores:     tb.stores, // REMOVED
		processors: tb.processors,
		sinks:      tb.sinks,
	}, nil
}

func (tb *TopologyBuilder) MustBuild() *Topology {
	topology, err := tb.Build()
	if err != nil {
		panic(err)
	}
	return topology
}

// Contains reports whether v is present in s.
func ContainsAny[E comparable](s []E, v []E) bool {
	for _, item := range s {
		for _, check := range v {
			if item == check {
				return true
			}
		}
	}

	return false
}

func NewTopologyBuilder() *TopologyBuilder {
	return &TopologyBuilder{
		graph:      NewTopologyGraph(),
		processors: map[string]*TopologyProcessor{},
		// stores:     map[string]*TopologyStore{}, // REMOVED
		sources:    map[string]*TopologySource{},
		sinks:      map[string]*TopologySink{},
	}
}

// TopologyStore - REMOVED, use StoreBuilder[StateStore] instead

type TopologySink struct {
	Name    string
	Builder func(*kgo.Client) Flusher
}

type TopologyProcessor struct {
	Name           string
	Build          func(stores map[string]Store) Node
	ChildNodeNames []string
	AddChildFunc   func(parent any, child any, childName string) // TODO - possible to do w/o parent ?
	StoreNames     []string
}

type TopologySource struct {
	Name           string
	Build          func() RawRecordProcessor
	ChildNodeNames []string
	AddChildFunc   func(parent any, child any, childName string) // TODO - possible to do w/o parent ?
}

func MustRegisterSource[K, V any](t *TopologyBuilder, name string, topic string, keyDeserializer Deserializer[K], valueDeserializer Deserializer[V]) {
	must(RegisterSource(t, name, topic, keyDeserializer, valueDeserializer))
}

func RegisterSource[K, V any](t *TopologyBuilder, name string, topic string, keyDeserializer Deserializer[K], valueDeserializer Deserializer[V]) error {
	nodeID := NodeID(name)

	// Check for duplicates in new graph
	if _, exists := t.graph.nodes[nodeID]; exists {
		return ErrNodeAlreadyExists
	}

	// Create new graph node using type-safe builder
	builder := &SourceNodeBuilder[K, V]{
		nodeID:            nodeID,
		topic:             topic,
		keyDeserializer:   keyDeserializer,
		valueDeserializer: valueDeserializer,
	}

	graphNode := builder.ToGraphNode()
	t.graph.nodes[nodeID] = graphNode
	t.graph.sources[topic] = nodeID
	t.graph.nodeOrder = append(t.graph.nodeOrder, nodeID) // Track insertion order!

	// Also keep old topology source for backward compatibility
	topoSource := &TopologySource{
		Name: name,
		Build: func() RawRecordProcessor {
			return &SourceNode[K, V]{KeyDeserializer: keyDeserializer, ValueDeserializer: valueDeserializer}
		},
		AddChildFunc: func(parent, child any, childName string) {
			parentNode, ok := parent.(*SourceNode[K, V])
			if !ok {
				panic("type error")
			}

			childNode, ok := child.(InputProcessor[K, V])
			if !ok {
				panic("type error")

			}

			parentNode.AddNext(childNode)

		},
		ChildNodeNames: []string{},
	}

	if _, found := t.processors[name]; found {
		return ErrNodeAlreadyExists
	}

	t.sources[name] = topoSource

	return nil
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func MustRegisterSink[K, V any](t *TopologyBuilder, name, topic string, keySerializer Serializer[K], valueSerializer Serializer[V], parent string) {
	must(RegisterSink(t, name, topic, keySerializer, valueSerializer, parent))
}

func RegisterSink[K, V any](t *TopologyBuilder, name, topic string, keySerializer Serializer[K], valueSerializer Serializer[V], parent string) error {
	nodeID := NodeID(name)
	parentID := NodeID(parent)

	// Check for duplicates in new graph
	if _, exists := t.graph.nodes[nodeID]; exists {
		return ErrNodeAlreadyExists
	}

	// Validate parent exists in new graph
	parentNode, ok := t.graph.nodes[parentID]
	if !ok {
		return ErrNodeNotFound
	}

	// Create new graph node using type-safe builder
	builder := &SinkNodeBuilder[K, V]{
		nodeID:          nodeID,
		topic:           topic,
		keySerializer:   keySerializer,
		valueSerializer: valueSerializer,
	}

	graphNode := builder.ToGraphNode()

	// Validate type compatibility
	if err := parentNode.ValidateDownstream(graphNode); err != nil {
		return fmt.Errorf("type mismatch connecting %s -> %s: %w", parent, name, err)
	}

	// Add to graph
	t.graph.nodes[nodeID] = graphNode
	t.graph.nodeOrder = append(t.graph.nodeOrder, nodeID)

	// Connect parent -> child in new graph
	parentNode.Children = append(parentNode.Children, nodeID)
	graphNode.Parents = append(graphNode.Parents, parentID)

	// Also keep old topology sink for backward compatibility
	topoSink := &TopologySink{
		Name: name,
		Builder: func(client *kgo.Client) Flusher {
			return NewSinkNode(client, topic, keySerializer, valueSerializer)
		},
	}

	t.sinks[name] = topoSink

	return SetParent(t, parent, name)
}

func MustRegisterProcessor[Kin, Vin, Kout, Vout any](t *TopologyBuilder, p ProcessorBuilder[Kin, Vin, Kout, Vout], name string, parent string, stores ...string) { // TODO: change to functional option for stores
	must(RegisterProcessor(t, p, name, parent, stores...))
}

func RegisterProcessor[Kin, Vin, Kout, Vout any](t *TopologyBuilder, p ProcessorBuilder[Kin, Vin, Kout, Vout], name string, parent string, stores ...string) error {
	nodeID := NodeID(name)
	parentID := NodeID(parent)

	// Check for duplicates in new graph
	if _, exists := t.graph.nodes[nodeID]; exists {
		return ErrNodeAlreadyExists
	}

	// Validate parent exists in new graph
	parentNode, ok := t.graph.nodes[parentID]
	if !ok {
		return ErrNodeNotFound
	}

	// Validate stores exist
	for _, storeName := range stores {
		if _, ok := t.graph.stores[storeName]; !ok {
			return errors.New("store not found")
		}
	}

	// Create new graph node using type-safe builder
	builder := &ProcessorNodeBuilder[Kin, Vin, Kout, Vout]{
		nodeID:        nodeID,
		processorFunc: p,
		storeNames:    stores,
	}

	graphNode := builder.ToGraphNode()

	// Validate type compatibility
	if err := parentNode.ValidateDownstream(graphNode); err != nil {
		return fmt.Errorf("type mismatch connecting %s -> %s: %w", parent, name, err)
	}

	// Add to graph
	t.graph.nodes[nodeID] = graphNode
	t.graph.nodeOrder = append(t.graph.nodeOrder, nodeID)

	// Connect parent -> child in new graph
	parentNode.Children = append(parentNode.Children, nodeID)
	graphNode.Parents = append(graphNode.Parents, parentID)

	// Also keep old topology processor for backward compatibility
	topoProcessor := &TopologyProcessor{
		Name: name,
		Build: func(stores map[string]Store) Node {
			node := &ProcessorNode[Kin, Vin, Kout, Vout]{
				userProcessor: p(),
				processorContext: &InternalProcessorContext[Kout, Vout]{
					outputs: map[string]InputProcessor[Kout, Vout]{},
					stores:  stores,
				},
			}

			return node
		},
		ChildNodeNames: []string{},
		StoreNames:     stores,
	}

	topoProcessor.AddChildFunc = func(parent any, child any, childName string) {
		parentNode, ok := parent.(*ProcessorNode[Kin, Vin, Kout, Vout])
		if !ok {
			panic("type error")
		}

		childNode, ok := child.(InputProcessor[Kout, Vout])
		if !ok {
			panic("type error")
		}

		parentNode.processorContext.outputs[childName] = childNode
	}

	if _, found := t.processors[name]; found {
		return ErrNodeAlreadyExists
	}

	t.processors[name] = topoProcessor

	return SetParent(t, parent, name)
}

func MustSetParent(t *TopologyBuilder, parent, child string) {
	must(SetParent(t, parent, child))
}

func SetParent(t *TopologyBuilder, parent, child string) error {
	parentNode, ok := t.processors[parent]
	if ok {
		parentNode.ChildNodeNames = append(parentNode.ChildNodeNames, child)
		return nil
	}

	source, ok := t.sources[parent]
	if ok {
		source.ChildNodeNames = append(source.ChildNodeNames, child)
		return nil
	}

	return ErrNodeNotFound
}

// RegisterRecordProcessor registers an enhanced processor that receives full Record objects
// with metadata (headers, timestamps, offsets, etc.)
func RegisterRecordProcessor[Kin, Vin, Kout, Vout any](
	t *TopologyBuilder,
	processorBuilder func() RecordProcessor[Kin, Vin, Kout, Vout],
	name string,
	parent string,
	stores ...string,
) error {
	// For now, enhanced RecordProcessors are registered the same way as legacy processors
	// The dual-interface support in RuntimeProcessorNode will handle the difference
	// This is a placeholder for future full integration
	// TODO: Create proper RecordProcessor builders in dag_builders.go
	return errors.New("RegisterRecordProcessor: full integration pending - use RegisterProcessor for now")
}

// RegisterRecordProcessorWithInterceptors registers an enhanced processor with interceptors
func RegisterRecordProcessorWithInterceptors[Kin, Vin, Kout, Vout any](
	t *TopologyBuilder,
	processorBuilder func() RecordProcessor[Kin, Vin, Kout, Vout],
	name string,
	parent string,
	interceptors []ProcessorInterceptor[Kin, Vin],
	stores ...string,
) error {
	// Wrap processor with interceptors
	wrappedBuilder := func() RecordProcessor[Kin, Vin, Kout, Vout] {
		return WithInterceptors(processorBuilder(), interceptors...)
	}

	return RegisterRecordProcessor(t, wrappedBuilder, name, parent, stores...)
}

var ErrNodeAlreadyExists = errors.New("node exists already")
var ErrNodeNotFound = errors.New("node not found")
var ErrInternal = errors.New("internal")
