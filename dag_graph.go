package kstreams

import (
	"github.com/twmb/franz-go/pkg/kgo"
)

// NodeID is a strongly-typed identifier for graph nodes
type NodeID string

// NodeType represents the kind of node in the topology
type NodeType int

const (
	NodeTypeSource NodeType = iota
	NodeTypeProcessor
	NodeTypeSink
)

// RuntimeNode is the interface that all instantiated nodes implement
type RuntimeNode interface {
	Init() error
	Close() error
}

// GraphNode is the build-time representation of a node in the topology DAG
// It stores type-erased builders and validation functions, but validates
// type compatibility at registration time
type GraphNode struct {
	ID   NodeID
	Type NodeType

	// Parent edges (incoming)
	Parents []NodeID

	// Child edges (outgoing)
	Children []NodeID

	// Type signature validation - set during registration
	// Returns error if attempting to connect incompatible types
	ValidateDownstream func(childNode *GraphNode) error

	// Builder creates the actual runtime node for a specific partition
	// Returns the instantiated node
	Build func(partition int32, stores map[string]Store, client *kgo.Client, stateManager *StateManager) (RuntimeNode, error)

	// Wiring function that connects this node to a child at runtime
	// Captures the proper types as closures during registration
	Wire func(parentRuntime RuntimeNode, childID NodeID, childRuntime RuntimeNode) error

	// Store dependencies
	StoreNames []string
}

// TopologyGraph is the build-time DAG representation
type TopologyGraph struct {
	nodes map[NodeID]*GraphNode

	// Track sources separately for quick access (topic -> nodeID)
	sources map[string]NodeID

	// Store definitions
	stores map[string]*StoreDefinition

	// Deterministic node ordering (insertion order)
	nodeOrder []NodeID
}

// StoreDefinition describes how to build a store
type StoreDefinition struct {
	Name    string
	Builder StoreBuilder[StateStore]
}

// NewTopologyGraph creates a new empty topology graph
func NewTopologyGraph() *TopologyGraph {
	return &TopologyGraph{
		nodes:     make(map[NodeID]*GraphNode),
		sources:   make(map[string]NodeID),
		stores:    make(map[string]*StoreDefinition),
		nodeOrder: make([]NodeID, 0),
	}
}
