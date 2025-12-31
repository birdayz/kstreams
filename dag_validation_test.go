package kstreams

import (
	"testing"

	"github.com/alecthomas/assert/v2"
)

// Test helper: create a simple graph node
func newTestNode(id string, nodeType NodeType, children ...string) *GraphNode {
	childIDs := make([]NodeID, len(children))
	for i, c := range children {
		childIDs[i] = NodeID(c)
	}
	return &GraphNode{
		ID:       NodeID(id),
		Type:     nodeType,
		Children: childIDs,
	}
}

// Test helper: build a graph from nodes
func buildTestGraph(nodes ...*GraphNode) *TopologyGraph {
	g := NewTopologyGraph()
	for _, node := range nodes {
		g.nodes[node.ID] = node
		// Add to parents for each child
		for _, childID := range node.Children {
			if child, ok := g.nodes[childID]; ok {
				child.Parents = append(child.Parents, node.ID)
			}
		}
	}
	// Second pass to fix parents for nodes added after their children
	for _, node := range nodes {
		for _, childID := range node.Children {
			if child, ok := g.nodes[childID]; ok {
				// Check if parent already exists
				hasParent := false
				for _, p := range child.Parents {
					if p == node.ID {
						hasParent = true
						break
					}
				}
				if !hasParent {
					child.Parents = append(child.Parents, node.ID)
				}
			}
		}
	}
	return g
}

// Test helper: add a source to graph
func addTestSource(g *TopologyGraph, id, topic string, children ...string) {
	node := newTestNode(id, NodeTypeSource, children...)
	g.nodes[node.ID] = node
	g.sources[topic] = node.ID
	// Update children parents
	for _, childID := range node.Children {
		if child, ok := g.nodes[childID]; ok {
			child.Parents = append(child.Parents, node.ID)
		}
	}
}

// Test helper: add a store to graph
func addTestStore(g *TopologyGraph, name string) {
	g.stores[name] = &StoreDefinition{
		Name:    name,
		Builder: nil, // nil is fine for validation tests
	}
}

func TestDetectCycles(t *testing.T) {
	tests := []struct {
		name    string
		graph   *TopologyGraph
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid DAG - linear chain",
			graph: buildTestGraph(
				newTestNode("A", NodeTypeSource, "B"),
				newTestNode("B", NodeTypeProcessor, "C"),
				newTestNode("C", NodeTypeSink),
			),
			wantErr: false,
		},
		{
			name: "valid DAG - branching",
			graph: buildTestGraph(
				newTestNode("A", NodeTypeSource, "B", "C"),
				newTestNode("B", NodeTypeProcessor, "D"),
				newTestNode("C", NodeTypeProcessor, "D"),
				newTestNode("D", NodeTypeSink),
			),
			wantErr: false,
		},
		{
			name: "simple cycle - A → B → A",
			graph: buildTestGraph(
				newTestNode("A", NodeTypeSource, "B"),
				newTestNode("B", NodeTypeProcessor, "A"),
			),
			wantErr: true,
			errMsg:  "cycle detected",
		},
		{
			name: "complex cycle - A → B → C → A",
			graph: buildTestGraph(
				newTestNode("A", NodeTypeSource, "B"),
				newTestNode("B", NodeTypeProcessor, "C"),
				newTestNode("C", NodeTypeProcessor, "A"),
			),
			wantErr: true,
			errMsg:  "cycle detected",
		},
		{
			name: "self-loop - A → A",
			graph: buildTestGraph(
				newTestNode("A", NodeTypeSource, "A"),
			),
			wantErr: true,
			errMsg:  "cycle detected",
		},
		{
			name: "cycle in one branch",
			graph: buildTestGraph(
				newTestNode("A", NodeTypeSource, "B", "C"),
				newTestNode("B", NodeTypeProcessor, "D"),
				newTestNode("C", NodeTypeProcessor, "E"),
				newTestNode("D", NodeTypeSink),
				newTestNode("E", NodeTypeProcessor, "C"), // Cycle: C → E → C
			),
			wantErr: true,
			errMsg:  "cycle detected",
		},
		{
			name:    "empty graph",
			graph:   NewTopologyGraph(),
			wantErr: false,
		},
		{
			name: "single node",
			graph: buildTestGraph(
				newTestNode("A", NodeTypeSource),
			),
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.graph.detectCycles()
			if tt.wantErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errMsg)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestValidateNoOrphans(t *testing.T) {
	tests := []struct {
		name    string
		setup   func() *TopologyGraph
		wantErr bool
		errMsg  string
	}{
		{
			name: "no orphans - all reachable from source",
			setup: func() *TopologyGraph {
				g := buildTestGraph(
					newTestNode("B", NodeTypeProcessor, "C"),
					newTestNode("C", NodeTypeSink),
				)
				addTestSource(g, "A", "topic1", "B")
				return g
			},
			wantErr: false,
		},
		{
			name: "multi-source - all reachable",
			setup: func() *TopologyGraph {
				g := buildTestGraph(
					newTestNode("B", NodeTypeProcessor, "D"),
					newTestNode("C", NodeTypeProcessor, "D"),
					newTestNode("D", NodeTypeSink),
				)
				addTestSource(g, "A1", "topic1", "B")
				addTestSource(g, "A2", "topic2", "C")
				return g
			},
			wantErr: false,
		},
		{
			name: "orphaned node - disconnected subgraph",
			setup: func() *TopologyGraph {
				g := buildTestGraph(
					newTestNode("B", NodeTypeProcessor, "C"),
					newTestNode("C", NodeTypeSink),
					newTestNode("D", NodeTypeProcessor, "E"), // Orphaned
					newTestNode("E", NodeTypeSink),           // Orphaned
				)
				addTestSource(g, "A", "topic1", "B")
				return g
			},
			wantErr: true,
			errMsg:  "orphaned nodes",
		},
		{
			name: "orphaned node - unreachable from source",
			setup: func() *TopologyGraph {
				g := buildTestGraph(
					newTestNode("B", NodeTypeProcessor, "C"),
					newTestNode("C", NodeTypeSink),
					newTestNode("D", NodeTypeProcessor), // Orphaned - no parents
				)
				addTestSource(g, "A", "topic1", "B")
				return g
			},
			wantErr: true,
			errMsg:  "orphaned nodes",
		},
		{
			name: "empty graph with no sources",
			setup: func() *TopologyGraph {
				return buildTestGraph(
					newTestNode("A", NodeTypeProcessor),
				)
			},
			wantErr: true,
			errMsg:  "orphaned nodes",
		},
		{
			name: "graph with only sources",
			setup: func() *TopologyGraph {
				g := NewTopologyGraph()
				addTestSource(g, "A", "topic1")
				return g
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			graph := tt.setup()
			err := graph.validateNoOrphans()
			if tt.wantErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errMsg)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestValidateSinks(t *testing.T) {
	tests := []struct {
		name    string
		graph   *TopologyGraph
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid sinks - no children",
			graph: buildTestGraph(
				newTestNode("A", NodeTypeSource, "B"),
				newTestNode("B", NodeTypeProcessor, "C"),
				newTestNode("C", NodeTypeSink), // Valid sink
			),
			wantErr: false,
		},
		{
			name: "multiple valid sinks",
			graph: buildTestGraph(
				newTestNode("A", NodeTypeSource, "B", "C"),
				newTestNode("B", NodeTypeSink), // Valid sink
				newTestNode("C", NodeTypeSink), // Valid sink
			),
			wantErr: false,
		},
		{
			name: "sink with children - invalid",
			graph: buildTestGraph(
				newTestNode("A", NodeTypeSource, "B"),
				newTestNode("B", NodeTypeSink, "C"), // Invalid - sink has child
				newTestNode("C", NodeTypeProcessor),
			),
			wantErr: true,
			errMsg:  "sink node B has children",
		},
		{
			name: "sink with multiple children - invalid",
			graph: buildTestGraph(
				newTestNode("A", NodeTypeSource, "B"),
				newTestNode("B", NodeTypeSink, "C", "D"), // Invalid
				newTestNode("C", NodeTypeProcessor),
				newTestNode("D", NodeTypeProcessor),
			),
			wantErr: true,
			errMsg:  "sink node B has children",
		},
		{
			name: "no sinks - valid",
			graph: buildTestGraph(
				newTestNode("A", NodeTypeSource, "B"),
				newTestNode("B", NodeTypeProcessor),
			),
			wantErr: false,
		},
		{
			name:    "empty graph",
			graph:   NewTopologyGraph(),
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.graph.validateSinks()
			if tt.wantErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errMsg)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestValidateStores(t *testing.T) {
	tests := []struct {
		name    string
		setup   func() *TopologyGraph
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid stores - all exist",
			setup: func() *TopologyGraph {
				g := NewTopologyGraph()
				node := newTestNode("A", NodeTypeProcessor)
				node.StoreNames = []string{"store1"}
				g.nodes[node.ID] = node
				addTestStore(g, "store1")
				return g
			},
			wantErr: false,
		},
		{
			name: "multiple stores - all valid",
			setup: func() *TopologyGraph {
				g := NewTopologyGraph()
				node := newTestNode("A", NodeTypeProcessor)
				node.StoreNames = []string{"store1", "store2"}
				g.nodes[node.ID] = node
				addTestStore(g, "store1")
				addTestStore(g, "store2")
				return g
			},
			wantErr: false,
		},
		{
			name: "missing store",
			setup: func() *TopologyGraph {
				g := NewTopologyGraph()
				node := newTestNode("A", NodeTypeProcessor)
				node.StoreNames = []string{"store1"}
				g.nodes[node.ID] = node
				// Don't add store1
				return g
			},
			wantErr: true,
			errMsg:  "undefined store store1",
		},
		{
			name: "some stores missing",
			setup: func() *TopologyGraph {
				g := NewTopologyGraph()
				node := newTestNode("A", NodeTypeProcessor)
				node.StoreNames = []string{"store1", "store2"}
				g.nodes[node.ID] = node
				addTestStore(g, "store1")
				// Don't add store2
				return g
			},
			wantErr: true,
			errMsg:  "undefined store store2",
		},
		{
			name: "no stores referenced - valid",
			setup: func() *TopologyGraph {
				g := NewTopologyGraph()
				node := newTestNode("A", NodeTypeProcessor)
				g.nodes[node.ID] = node
				return g
			},
			wantErr: false,
		},
		{
			name: "empty StoreNames slice - valid",
			setup: func() *TopologyGraph {
				g := NewTopologyGraph()
				node := newTestNode("A", NodeTypeProcessor)
				node.StoreNames = []string{}
				g.nodes[node.ID] = node
				return g
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			graph := tt.setup()
			err := graph.validateStores()
			if tt.wantErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errMsg)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestTopologicalSort(t *testing.T) {
	tests := []struct {
		name    string
		graph   *TopologyGraph
		wantErr bool
		errMsg  string
		// For valid cases, verify order properties
		checkOrder func(*testing.T, []NodeID)
	}{
		{
			name: "linear chain",
			graph: buildTestGraph(
				newTestNode("A", NodeTypeSource, "B"),
				newTestNode("B", NodeTypeProcessor, "C"),
				newTestNode("C", NodeTypeSink),
			),
			wantErr: false,
			checkOrder: func(t *testing.T, order []NodeID) {
				assert.Equal(t, 3, len(order))
				// A must come before B, B before C
				aIdx, bIdx, cIdx := -1, -1, -1
				for i, id := range order {
					switch id {
					case "A":
						aIdx = i
					case "B":
						bIdx = i
					case "C":
						cIdx = i
					}
				}
				assert.True(t, aIdx < bIdx)
				assert.True(t, bIdx < cIdx)
			},
		},
		{
			name: "diamond topology",
			graph: buildTestGraph(
				newTestNode("A", NodeTypeSource, "B", "C"),
				newTestNode("B", NodeTypeProcessor, "D"),
				newTestNode("C", NodeTypeProcessor, "D"),
				newTestNode("D", NodeTypeSink),
			),
			wantErr: false,
			checkOrder: func(t *testing.T, order []NodeID) {
				assert.Equal(t, 4, len(order))
				// A must come before B,C,D; B,C must come before D
				indices := make(map[NodeID]int)
				for i, id := range order {
					indices[id] = i
				}
				assert.True(t, indices["A"] < indices["B"])
				assert.True(t, indices["A"] < indices["C"])
				assert.True(t, indices["A"] < indices["D"])
				assert.True(t, indices["B"] < indices["D"])
				assert.True(t, indices["C"] < indices["D"])
			},
		},
		{
			name: "graph with cycle",
			graph: buildTestGraph(
				newTestNode("A", NodeTypeSource, "B"),
				newTestNode("B", NodeTypeProcessor, "C"),
				newTestNode("C", NodeTypeProcessor, "A"), // Cycle
			),
			wantErr: true,
			errMsg:  "cycles",
		},
		{
			name: "determinism test - same graph should produce same order",
			graph: buildTestGraph(
				newTestNode("A", NodeTypeSource, "C", "D"),
				newTestNode("B", NodeTypeSource, "C", "D"),
				newTestNode("C", NodeTypeProcessor, "E"),
				newTestNode("D", NodeTypeProcessor, "E"),
				newTestNode("E", NodeTypeSink),
			),
			wantErr: false,
			checkOrder: func(t *testing.T, order []NodeID) {
				// Run multiple times to verify determinism
				for i := 0; i < 5; i++ {
					g := buildTestGraph(
						newTestNode("A", NodeTypeSource, "C", "D"),
						newTestNode("B", NodeTypeSource, "C", "D"),
						newTestNode("C", NodeTypeProcessor, "E"),
						newTestNode("D", NodeTypeProcessor, "E"),
						newTestNode("E", NodeTypeSink),
					)
					newOrder, err := g.topologicalSort()
					assert.NoError(t, err)
					assert.Equal(t, order, newOrder, "topological sort must be deterministic")
				}
			},
		},
		{
			name:    "empty graph",
			graph:   NewTopologyGraph(),
			wantErr: false,
			checkOrder: func(t *testing.T, order []NodeID) {
				assert.Equal(t, 0, len(order))
			},
		},
		{
			name: "single node",
			graph: buildTestGraph(
				newTestNode("A", NodeTypeSource),
			),
			wantErr: false,
			checkOrder: func(t *testing.T, order []NodeID) {
				assert.Equal(t, 1, len(order))
				assert.Equal(t, NodeID("A"), order[0])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			order, err := tt.graph.topologicalSort()
			if tt.wantErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errMsg)
			} else {
				assert.NoError(t, err)
				if tt.checkOrder != nil {
					tt.checkOrder(t, order)
				}
			}
		})
	}
}

func TestComputePartitionGroups(t *testing.T) {
	tests := []struct {
		name       string
		setup      func() *TopologyGraph
		wantErr    bool
		checkGroups func(*testing.T, []*PartitionGroup)
	}{
		{
			name: "single source - single partition group",
			setup: func() *TopologyGraph {
				g := buildTestGraph(
					newTestNode("proc1", NodeTypeProcessor, "sink1"),
					newTestNode("sink1", NodeTypeSink),
				)
				addTestSource(g, "source1", "topic1", "proc1")
				return g
			},
			wantErr: false,
			checkGroups: func(t *testing.T, groups []*PartitionGroup) {
				assert.Equal(t, 1, len(groups))
				assert.Equal(t, []string{"topic1"}, groups[0].sourceTopics)
			},
		},
		{
			name: "multiple sources - separate partition groups",
			setup: func() *TopologyGraph {
				g := buildTestGraph(
					newTestNode("proc1", NodeTypeProcessor, "sink1"),
					newTestNode("proc2", NodeTypeProcessor, "sink2"),
					newTestNode("sink1", NodeTypeSink),
					newTestNode("sink2", NodeTypeSink),
				)
				addTestSource(g, "source1", "topic1", "proc1")
				addTestSource(g, "source2", "topic2", "proc2")
				return g
			},
			wantErr: false,
			checkGroups: func(t *testing.T, groups []*PartitionGroup) {
				assert.Equal(t, 2, len(groups))
				// Check that topics are distributed
				topics := make(map[string]bool)
				for _, g := range groups {
					for _, topic := range g.sourceTopics {
						topics[topic] = true
					}
				}
				assert.True(t, topics["topic1"])
				assert.True(t, topics["topic2"])
			},
		},
		{
			name: "processors with stores",
			setup: func() *TopologyGraph {
				g := buildTestGraph(
					newTestNode("sink1", NodeTypeSink),
				)
				proc := newTestNode("proc1", NodeTypeProcessor, "sink1")
				proc.StoreNames = []string{"store1"}
				g.nodes[proc.ID] = proc
				addTestSource(g, "source1", "topic1", "proc1")
				addTestStore(g, "store1")
				return g
			},
			wantErr: false,
			checkGroups: func(t *testing.T, groups []*PartitionGroup) {
				assert.Equal(t, 1, len(groups))
				assert.Equal(t, []string{"store1"}, groups[0].storeNames)
			},
		},
		{
			name: "determinism - same graph produces same groups",
			setup: func() *TopologyGraph {
				g := buildTestGraph(
					newTestNode("proc1", NodeTypeProcessor, "sink1"),
					newTestNode("proc2", NodeTypeProcessor, "sink2"),
					newTestNode("sink1", NodeTypeSink),
					newTestNode("sink2", NodeTypeSink),
				)
				addTestSource(g, "source1", "topic1", "proc1")
				addTestSource(g, "source2", "topic2", "proc2")
				return g
			},
			wantErr: false,
			checkGroups: func(t *testing.T, groups []*PartitionGroup) {
				// Run multiple times to verify determinism
				for i := 0; i < 5; i++ {
					g := buildTestGraph(
						newTestNode("proc1", NodeTypeProcessor, "sink1"),
						newTestNode("proc2", NodeTypeProcessor, "sink2"),
						newTestNode("sink1", NodeTypeSink),
						newTestNode("sink2", NodeTypeSink),
					)
					addTestSource(g, "source1", "topic1", "proc1")
					addTestSource(g, "source2", "topic2", "proc2")
					newGroups, err := g.ComputePartitionGroups()
					assert.NoError(t, err)
					assert.Equal(t, len(groups), len(newGroups), "partition groups must be deterministic")
				}
			},
		},
		{
			name: "empty graph",
			setup: func() *TopologyGraph {
				return NewTopologyGraph()
			},
			wantErr: false,
			checkGroups: func(t *testing.T, groups []*PartitionGroup) {
				assert.Equal(t, 0, len(groups))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			graph := tt.setup()
			groups, err := graph.ComputePartitionGroups()
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				if tt.checkGroups != nil {
					tt.checkGroups(t, groups)
				}
			}
		})
	}
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name    string
		setup   func() *TopologyGraph
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid topology - all validations pass",
			setup: func() *TopologyGraph {
				g := buildTestGraph(
					newTestNode("proc1", NodeTypeProcessor, "sink1"),
					newTestNode("sink1", NodeTypeSink),
				)
				addTestSource(g, "source1", "topic1", "proc1")
				return g
			},
			wantErr: false,
		},
		{
			name: "invalid - has cycle",
			setup: func() *TopologyGraph {
				g := buildTestGraph(
					newTestNode("proc1", NodeTypeProcessor, "proc2"),
					newTestNode("proc2", NodeTypeProcessor, "proc1"), // Cycle
				)
				addTestSource(g, "source1", "topic1", "proc1")
				return g
			},
			wantErr: true,
			errMsg:  "cycle detected",
		},
		{
			name: "invalid - has orphans",
			setup: func() *TopologyGraph {
				g := buildTestGraph(
					newTestNode("proc1", NodeTypeProcessor, "sink1"),
					newTestNode("proc2", NodeTypeProcessor), // Orphan
					newTestNode("sink1", NodeTypeSink),
				)
				addTestSource(g, "source1", "topic1", "proc1")
				return g
			},
			wantErr: true,
			errMsg:  "orphaned nodes",
		},
		{
			name: "invalid - sink has children",
			setup: func() *TopologyGraph {
				g := buildTestGraph(
					newTestNode("proc1", NodeTypeProcessor, "sink1"),
					newTestNode("sink1", NodeTypeSink, "proc2"), // Invalid sink
					newTestNode("proc2", NodeTypeProcessor),
				)
				addTestSource(g, "source1", "topic1", "proc1")
				return g
			},
			wantErr: true,
			errMsg:  "sink node sink1 has children",
		},
		{
			name: "invalid - missing store",
			setup: func() *TopologyGraph {
				g := buildTestGraph(
					newTestNode("sink1", NodeTypeSink),
				)
				proc := newTestNode("proc1", NodeTypeProcessor, "sink1")
				proc.StoreNames = []string{"missing-store"}
				g.nodes[proc.ID] = proc
				addTestSource(g, "source1", "topic1", "proc1")
				return g
			},
			wantErr: true,
			errMsg:  "undefined store",
		},
		{
			name: "multiple errors - returns all",
			setup: func() *TopologyGraph {
				g := buildTestGraph(
					newTestNode("proc1", NodeTypeProcessor, "proc1"), // Cycle (self-loop)
					newTestNode("proc2", NodeTypeProcessor),          // Orphan
				)
				addTestSource(g, "source1", "topic1", "proc1")
				return g
			},
			wantErr: true,
			// Should contain multiple error messages joined
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			graph := tt.setup()
			err := graph.Validate()
			if tt.wantErr {
				assert.Error(t, err)
				if tt.errMsg != "" {
					assert.Contains(t, err.Error(), tt.errMsg)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
