package kstreams

import (
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

// SourceNodeBuilder builds a source node with type safety
type SourceNodeBuilder[K, V any] struct {
	nodeID            NodeID
	topic             string
	keyDeserializer   Deserializer[K]
	valueDeserializer Deserializer[V]
}

// ToGraphNode converts the typed builder to a type-erased GraphNode
func (b *SourceNodeBuilder[K, V]) ToGraphNode() *GraphNode {
	return &GraphNode{
		ID:         b.nodeID,
		Type:       NodeTypeSource,
		Parents:    []NodeID{},
		Children:   []NodeID{},
		StoreNames: []string{},

		// Validation function - this will be called when adding children
		ValidateDownstream: func(child *GraphNode) error {
			// Source nodes can connect to processors or sinks
			// We can't do full type validation here, but we can check node types
			if child.Type != NodeTypeProcessor && child.Type != NodeTypeSink {
				return fmt.Errorf("source node can only connect to processor or sink nodes")
			}
			return nil
		},

		// Build function creates the runtime source node
		Build: func(partition int32, stores map[string]Store, client *kgo.Client, stateManager *StateManager) (RuntimeNode, error) {
			return &RuntimeSourceNode[K, V]{
				id:                b.nodeID,
				topic:             b.topic,
				keyDeserializer:   b.keyDeserializer,
				valueDeserializer: b.valueDeserializer,
				downstream:        make([]InputProcessor[K, V], 0),
			}, nil
		},

		// Wire function connects this source to a child at runtime
		Wire: func(parentRuntime RuntimeNode, childID NodeID, childRuntime RuntimeNode) error {
			parent, ok := parentRuntime.(*RuntimeSourceNode[K, V])
			if !ok {
				return fmt.Errorf("parent is not a RuntimeSourceNode[K,V]")
			}

			child, ok := childRuntime.(InputProcessor[K, V])
			if !ok {
				return fmt.Errorf("child does not implement InputProcessor[K,V]")
			}

			parent.downstream = append(parent.downstream, child)
			return nil
		},
	}
}

// ProcessorNodeBuilder builds a processor node with type safety
type ProcessorNodeBuilder[Kin, Vin, Kout, Vout any] struct {
	nodeID        NodeID
	processorFunc ProcessorBuilder[Kin, Vin, Kout, Vout]
	storeNames    []string
}

// ToGraphNode converts the typed builder to a type-erased GraphNode
func (b *ProcessorNodeBuilder[Kin, Vin, Kout, Vout]) ToGraphNode() *GraphNode {
	return &GraphNode{
		ID:         b.nodeID,
		Type:       NodeTypeProcessor,
		Parents:    []NodeID{},
		Children:   []NodeID{},
		StoreNames: b.storeNames,

		// Validation function
		ValidateDownstream: func(child *GraphNode) error {
			// Processor nodes can connect to other processors or sinks
			if child.Type != NodeTypeProcessor && child.Type != NodeTypeSink {
				return fmt.Errorf("processor node can only connect to processor or sink nodes")
			}
			return nil
		},

		// Build function creates the runtime processor node
		Build: func(partition int32, stores map[string]Store, client *kgo.Client, stateManager *StateManager) (RuntimeNode, error) {
			// Build processor with stores
			processorStores := make(map[string]Store)
			for _, storeName := range b.storeNames {
				if store, ok := stores[storeName]; ok {
					processorStores[storeName] = store
				} else {
					return nil, fmt.Errorf("store %s not found", storeName)
				}
			}

			node := &RuntimeProcessorNode[Kin, Vin, Kout, Vout]{
				id:            b.nodeID,
				userProcessor: b.processorFunc(),
				context: &IsolatedProcessorContext[Kout, Vout]{
					nodeID:       b.nodeID,
					outputs:      make(map[NodeID]InputProcessor[Kout, Vout]),
					stores:       processorStores,
					errors:       make([]error, 0),
					stateManager: stateManager,
					client:       client,
				},
			}

			return node, nil
		},

		// Wire function connects this processor to a child at runtime
		Wire: func(parentRuntime RuntimeNode, childID NodeID, childRuntime RuntimeNode) error {
			parent, ok := parentRuntime.(*RuntimeProcessorNode[Kin, Vin, Kout, Vout])
			if !ok {
				return fmt.Errorf("parent is not a RuntimeProcessorNode[Kin,Vin,Kout,Vout]")
			}

			child, ok := childRuntime.(InputProcessor[Kout, Vout])
			if !ok {
				return fmt.Errorf("child does not implement InputProcessor[Kout,Vout]")
			}

			parent.context.outputs[childID] = child
			return nil
		},
	}
}

// SinkNodeBuilder builds a sink with type safety
type SinkNodeBuilder[K, V any] struct {
	nodeID          NodeID
	topic           string
	keySerializer   Serializer[K]
	valueSerializer Serializer[V]
}

// ToGraphNode converts the typed builder to a type-erased GraphNode
func (b *SinkNodeBuilder[K, V]) ToGraphNode() *GraphNode {
	return &GraphNode{
		ID:         b.nodeID,
		Type:       NodeTypeSink,
		Parents:    []NodeID{},
		Children:   []NodeID{},
		StoreNames: []string{},

		// Validation function - sinks cannot have children
		ValidateDownstream: func(child *GraphNode) error {
			return fmt.Errorf("sink nodes cannot have children")
		},

		// Build function creates the runtime sink node
		Build: func(partition int32, stores map[string]Store, client *kgo.Client, stateManager *StateManager) (RuntimeNode, error) {
			return &RuntimeSinkNode[K, V]{
				id:              b.nodeID,
				client:          client,
				topic:           b.topic,
				keySerializer:   b.keySerializer,
				valueSerializer: b.valueSerializer,
				futures:         make([]produceResult, 0),
			}, nil
		},

		// Wire function - sinks don't wire to children (they're terminal nodes)
		Wire: func(parentRuntime RuntimeNode, childID NodeID, childRuntime RuntimeNode) error {
			return fmt.Errorf("sink nodes cannot have children")
		},
	}
}
