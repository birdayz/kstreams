package kdag

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/birdayz/kstreams/kstate"
)

// RuntimeBuilder is implemented by execution package builders.
// This provides compile-time safety that only valid builders are stored in nodes.
// The interface is defined here to avoid import cycles (kdag can't import execution).
type RuntimeBuilder interface {
	// BuilderKind returns what type of node this builder creates.
	BuilderKind() NodeType
}

// StoreBuilder is implemented by state store builders.
// This provides compile-time safety for store definitions.
type StoreBuilder interface {
	// StoreName returns the name of the store this builder creates.
	StoreName() string
}

// NodeID is a strongly-typed identifier for graph nodes.
// NodeIDs must be non-empty and cannot contain whitespace.
type NodeID string

// Validate checks if the NodeID is valid.
// Returns ErrInvalidNodeID if the ID is empty or contains whitespace.
func (id NodeID) Validate() error {
	if id == "" {
		return fmt.Errorf("%w: NodeID cannot be empty", ErrInvalidNodeID)
	}
	if strings.ContainsAny(string(id), " \t\n\r") {
		return fmt.Errorf("%w: NodeID %q cannot contain whitespace", ErrInvalidNodeID, id)
	}
	return nil
}

// NodeType represents the kind of node in the DAG
type NodeType int

const (
	NodeTypeSource NodeType = iota
	NodeTypeProcessor
	NodeTypeSink
)

func (t NodeType) String() string {
	switch t {
	case NodeTypeSource:
		return "Source"
	case NodeTypeProcessor:
		return "Processor"
	case NodeTypeSink:
		return "Sink"
	default:
		return "Unknown"
	}
}

// Node is the build-time representation of a node in the DAG.
// It contains only metadata needed for validation and graph operations.
// Runtime construction is handled by the execution package.
type Node struct {
	ID   NodeID
	Type NodeType

	// Parent edges (incoming)
	Parents []NodeID

	// Child edges (outgoing)
	Children []NodeID

	// Type signatures for compile-time type checking
	// InputKeyType and InputValueType describe what types this node accepts
	// OutputKeyType and OutputValueType describe what types this node produces
	InputKeyType    reflect.Type
	InputValueType  reflect.Type
	OutputKeyType   reflect.Type
	OutputValueType reflect.Type

	// Store dependencies (just names, not actual stores)
	StoreNames []string

	// RuntimeBuilder is used by execution.BuildTaskFromGraph to construct runtime nodes.
	// Implements the RuntimeBuilder interface defined in this package.
	RuntimeBuilder RuntimeBuilder
}

// ValidateDownstream checks if this node can connect to the given child node.
// Returns ErrTypeMismatch if types are incompatible.
func (n *Node) ValidateDownstream(child *Node) error {
	// Check node type compatibility
	if n.Type == NodeTypeSink {
		return fmt.Errorf("%w: sink nodes cannot have children", ErrInvalidTopology)
	}
	if child.Type == NodeTypeSource {
		return fmt.Errorf("%w: source nodes cannot be children", ErrInvalidTopology)
	}

	// Check type compatibility (output of parent must match input of child)
	if n.OutputKeyType != child.InputKeyType {
		return fmt.Errorf("%w: %s outputs key type %v but %s expects %v",
			ErrTypeMismatch, n.ID, n.OutputKeyType, child.ID, child.InputKeyType)
	}
	if n.OutputValueType != child.InputValueType {
		return fmt.Errorf("%w: %s outputs value type %v but %s expects %v",
			ErrTypeMismatch, n.ID, n.OutputValueType, child.ID, child.InputValueType)
	}

	return nil
}

// Graph is the build-time DAG representation.
// It contains only structural information - no runtime behavior.
type Graph struct {
	Nodes map[NodeID]*Node

	// Track sources separately for quick access (topic -> nodeID)
	Sources map[string]NodeID

	// Store definitions (name -> definition)
	Stores map[string]*StoreDefinition

	// Deterministic node ordering (insertion order)
	NodeOrder []NodeID
}

// StoreDefinition describes a state store.
type StoreDefinition struct {
	Name    string
	Builder kstate.TypeErasedStoreBuilder // Type-safe interface for store builders
}

// NewGraph creates a new empty graph.
func NewGraph() *Graph {
	return &Graph{
		Nodes:     make(map[NodeID]*Node),
		Sources:   make(map[string]NodeID),
		Stores:    make(map[string]*StoreDefinition),
		NodeOrder: make([]NodeID, 0),
	}
}

// AddNode adds a node to the graph.
func (g *Graph) AddNode(node *Node) error {
	if err := node.ID.Validate(); err != nil {
		return err
	}
	if _, exists := g.Nodes[node.ID]; exists {
		return fmt.Errorf("%w: %s", ErrNodeAlreadyExists, node.ID)
	}
	g.Nodes[node.ID] = node
	g.NodeOrder = append(g.NodeOrder, node.ID)
	return nil
}

// AddEdge adds a directed edge from parent to child.
// Validates type compatibility before adding.
func (g *Graph) AddEdge(parentID, childID NodeID) error {
	parent, ok := g.Nodes[parentID]
	if !ok {
		return fmt.Errorf("%w: parent %s", ErrNodeNotFound, parentID)
	}
	child, ok := g.Nodes[childID]
	if !ok {
		return fmt.Errorf("%w: child %s", ErrNodeNotFound, childID)
	}

	// Validate type compatibility
	if err := parent.ValidateDownstream(child); err != nil {
		return fmt.Errorf("cannot connect %s -> %s: %w", parentID, childID, err)
	}

	parent.Children = append(parent.Children, childID)
	child.Parents = append(child.Parents, parentID)
	return nil
}

// ReverseTopologicalSort returns nodes in reverse topological order.
// This means children come before parents - useful for bottom-up construction.
func (g *Graph) ReverseTopologicalSort() ([]NodeID, error) {
	order, err := g.topologicalSort()
	if err != nil {
		return nil, err
	}

	// Reverse the order
	for i, j := 0, len(order)-1; i < j; i, j = i+1, j-1 {
		order[i], order[j] = order[j], order[i]
	}
	return order, nil
}

// GetSourceTopic returns the topic name for a source node, or empty string if not found.
func (g *Graph) GetSourceTopic(nodeID NodeID) string {
	for topic, srcID := range g.Sources {
		if srcID == nodeID {
			return topic
		}
	}
	return ""
}
