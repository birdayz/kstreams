package kdag

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/birdayz/kstreams/kstate"
)

// Builder constructs a processing DAG.
//
// IMPORTANT: Builder is NOT safe for concurrent use. All registration
// methods must be called from a single goroutine. The resulting DAG
// is immutable and safe to use concurrently.
//
// Registration functions are in the execution package (RegisterSource,
// RegisterProcessor, RegisterSink) because they need to create typed
// runtime builders. This package provides structural methods for the
// execution package to use.
type Builder struct {
	graph *Graph
}

// NewBuilder creates a new DAG builder.
func NewBuilder() *Builder {
	return &Builder{
		graph: NewGraph(),
	}
}

// Build validates and finalizes the DAG.
func (b *Builder) Build() (*DAG, error) {
	if err := b.graph.Validate(); err != nil {
		return nil, err
	}

	partitionGroups, err := b.graph.ComputePartitionGroups()
	if err != nil {
		return nil, fmt.Errorf("failed to compute partition groups: %w", err)
	}

	return &DAG{
		graph:           b.graph,
		partitionGroups: partitionGroups,
	}, nil
}

// MustBuild is like Build but panics on error.
func (b *Builder) MustBuild() *DAG {
	dag, err := b.Build()
	if err != nil {
		panic(err)
	}
	return dag
}

// GetGraph returns the underlying graph for read-only access.
func (b *Builder) GetGraph() *Graph {
	return b.graph
}

// GetNode returns a node by ID if it exists.
func (b *Builder) GetNode(id NodeID) (*Node, bool) {
	node, ok := b.graph.Nodes[id]
	return node, ok
}

// AddSourceNode adds a source node to the graph.
// Called by execution.RegisterSource with typed runtime builder.
func (b *Builder) AddSourceNode(name, topic string, keyType, valueType reflect.Type, builder RuntimeBuilder) error {
	nodeID := NodeID(name)

	if err := nodeID.Validate(); err != nil {
		return err
	}

	if _, exists := b.graph.Nodes[nodeID]; exists {
		return fmt.Errorf("%w: source %q", ErrNodeAlreadyExists, name)
	}

	node := &Node{
		ID:              nodeID,
		Type:            NodeTypeSource,
		Parents:         []NodeID{},
		Children:        []NodeID{},
		StoreNames:      []string{},
		InputKeyType:    nil, // Sources don't have input
		InputValueType:  nil,
		OutputKeyType:   keyType,
		OutputValueType: valueType,
		RuntimeBuilder:  builder,
	}

	b.graph.Nodes[nodeID] = node
	b.graph.Sources[topic] = nodeID
	b.graph.NodeOrder = append(b.graph.NodeOrder, nodeID)

	return nil
}

// AddProcessorNode adds a processor node to the graph.
// Called by execution.RegisterProcessor with typed runtime builder.
func (b *Builder) AddProcessorNode(
	name, parent string,
	keyInType, valueInType, keyOutType, valueOutType reflect.Type,
	stores []string,
	builder RuntimeBuilder,
	parentNode *Node,
) error {
	nodeID := NodeID(name)
	parentID := NodeID(parent)

	node := &Node{
		ID:              nodeID,
		Type:            NodeTypeProcessor,
		Parents:         []NodeID{},
		Children:        []NodeID{},
		StoreNames:      stores,
		InputKeyType:    keyInType,
		InputValueType:  valueInType,
		OutputKeyType:   keyOutType,
		OutputValueType: valueOutType,
		RuntimeBuilder:  builder,
	}

	// Validate type compatibility
	if err := parentNode.ValidateDownstream(node); err != nil {
		return fmt.Errorf("cannot connect %s -> %s: %w", parent, name, err)
	}

	b.graph.Nodes[nodeID] = node
	b.graph.NodeOrder = append(b.graph.NodeOrder, nodeID)

	parentNode.Children = append(parentNode.Children, nodeID)
	node.Parents = append(node.Parents, parentID)

	return nil
}

// AddSinkNode adds a sink node to the graph.
// Called by execution.RegisterSink with typed runtime builder.
func (b *Builder) AddSinkNode(
	name, topic, parent string,
	keyType, valueType reflect.Type,
	builder RuntimeBuilder,
	parentNode *Node,
) error {
	nodeID := NodeID(name)
	parentID := NodeID(parent)

	node := &Node{
		ID:              nodeID,
		Type:            NodeTypeSink,
		Parents:         []NodeID{},
		Children:        []NodeID{},
		StoreNames:      []string{},
		InputKeyType:    keyType,
		InputValueType:  valueType,
		OutputKeyType:   nil, // Sinks don't have output
		OutputValueType: nil,
		RuntimeBuilder:  builder,
	}

	// Validate type compatibility
	if err := parentNode.ValidateDownstream(node); err != nil {
		return fmt.Errorf("cannot connect %s -> %s: %w", parent, name, err)
	}

	b.graph.Nodes[nodeID] = node
	b.graph.NodeOrder = append(b.graph.NodeOrder, nodeID)

	parentNode.Children = append(parentNode.Children, nodeID)
	node.Parents = append(node.Parents, parentID)

	return nil
}

// RegisterStore registers a state store with the DAG.
func RegisterStore(b *Builder, sb kstate.TypeErasedStoreBuilder, name string) error {
	if _, exists := b.graph.Stores[name]; exists {
		return fmt.Errorf("%w: store %q", ErrNodeAlreadyExists, name)
	}
	b.graph.Stores[name] = &StoreDefinition{
		Name:    name,
		Builder: sb,
	}
	return nil
}

// MustRegisterStore is like RegisterStore but panics on error.
func MustRegisterStore(b *Builder, sb kstate.TypeErasedStoreBuilder, name string) {
	must(RegisterStore(b, sb, name))
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

// Sentinel errors for common failure cases.
var (
	ErrNodeAlreadyExists = errors.New("node already exists")
	ErrNodeNotFound      = errors.New("node not found")
	ErrCycleDetected     = errors.New("cycle detected in DAG")
	ErrOrphanedNodes     = errors.New("orphaned nodes found")
	ErrStoreNotFound     = errors.New("store not found")
	ErrInvalidNodeID     = errors.New("invalid node ID")
	ErrTypeMismatch      = errors.New("type mismatch")
	ErrInvalidTopology   = errors.New("invalid topology")
)
