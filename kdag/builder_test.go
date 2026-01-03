package kdag

import (
	"errors"
	"fmt"
	"testing"

	"github.com/alecthomas/assert/v2"
)

func TestNewBuilder(t *testing.T) {
	tb := NewBuilder()
	assert.NotZero(t, tb)
	assert.NotZero(t, tb.GetGraph())
	// Maps are initialized (not nil)
	assert.NotEqual(t, (map[NodeID]*Node)(nil), tb.GetGraph().Nodes)
	assert.NotEqual(t, (map[string]*StoreDefinition)(nil), tb.GetGraph().Stores)
	assert.NotEqual(t, (map[string]NodeID)(nil), tb.GetGraph().Sources)
}

func TestAddSourceNode(t *testing.T) {
	t.Run("valid source registration", func(t *testing.T) {
		tb := NewBuilder()
		err := registerTestSource(tb, "source1", "topic1")
		assert.NoError(t, err)

		// Verify source was registered in graph
		node, exists := tb.GetGraph().Nodes[NodeID("source1")]
		assert.True(t, exists)
		assert.Equal(t, NodeTypeSource, node.Type)

		// Verify topic mapping
		sourceID, exists := tb.GetGraph().Sources["topic1"]
		assert.True(t, exists)
		assert.Equal(t, NodeID("source1"), sourceID)
	})

	t.Run("duplicate source name", func(t *testing.T) {
		tb := NewBuilder()
		err := registerTestSource(tb, "source1", "topic1")
		assert.NoError(t, err)

		// Try to register again with same name
		err = registerTestSource(tb, "source1", "topic2")
		assert.Error(t, err)
		assert.True(t, errors.Is(err, ErrNodeAlreadyExists))
	})

	t.Run("multiple sources - different names", func(t *testing.T) {
		tb := NewBuilder()
		err := registerTestSource(tb, "source1", "topic1")
		assert.NoError(t, err)

		err = registerTestSource(tb, "source2", "topic2")
		assert.NoError(t, err)

		assert.Equal(t, 2, len(tb.GetGraph().Nodes))
		assert.Equal(t, 2, len(tb.GetGraph().Sources))
	})
}

func TestAddProcessorNode(t *testing.T) {
	t.Run("valid processor registration", func(t *testing.T) {
		tb := NewBuilder()
		assert.NoError(t, registerTestSource(tb, "source", "input"))

		err := registerTestProcessor(tb, "processor1", "source")
		assert.NoError(t, err)

		// Verify processor was registered in graph
		node, exists := tb.GetGraph().Nodes[NodeID("processor1")]
		assert.True(t, exists)
		assert.Equal(t, NodeTypeProcessor, node.Type)

		// Verify parent-child relationship
		sourceNode := tb.GetGraph().Nodes[NodeID("source")]
		assert.Equal(t, []NodeID{NodeID("processor1")}, sourceNode.Children)
		assert.Equal(t, []NodeID{NodeID("source")}, node.Parents)
	})

	t.Run("parent not found", func(t *testing.T) {
		tb := NewBuilder()

		err := registerTestProcessor(tb, "processor1", "nonexistent")
		assert.Error(t, err)
		assert.True(t, errors.Is(err, ErrNodeNotFound))
	})

	t.Run("duplicate processor name", func(t *testing.T) {
		tb := NewBuilder()
		assert.NoError(t, registerTestSource(tb, "source", "input"))

		err := registerTestProcessor(tb, "processor1", "source")
		assert.NoError(t, err)

		// Try to register again with same name - will fail during AddProcessorNode
		err = registerTestProcessor(tb, "processor1", "source")
		assert.Error(t, err)
	})

	t.Run("processor with valid stores", func(t *testing.T) {
		tb := NewBuilder()
		assert.NoError(t, registerTestSource(tb, "source", "input"))

		// Register a store
		tb.GetGraph().Stores["store1"] = &StoreDefinition{Name: "store1"}

		err := registerTestProcessor(tb, "processor1", "source", "store1")
		assert.NoError(t, err)

		// Verify store reference
		node := tb.GetGraph().Nodes[NodeID("processor1")]
		assert.Equal(t, []string{"store1"}, node.StoreNames)
	})

	t.Run("processor with missing store", func(t *testing.T) {
		tb := NewBuilder()
		assert.NoError(t, registerTestSource(tb, "source", "input"))

		err := registerTestProcessor(tb, "processor1", "source", "nonexistent_store")
		assert.Error(t, err)
		assert.True(t, errors.Is(err, ErrStoreNotFound))
	})

	t.Run("chain multiple processors", func(t *testing.T) {
		tb := NewBuilder()
		assert.NoError(t, registerTestSource(tb, "source", "input"))

		assert.NoError(t, registerTestProcessor(tb, "processor1", "source"))
		assert.NoError(t, registerTestProcessor(tb, "processor2", "processor1"))

		// Verify chain
		p1 := tb.GetGraph().Nodes[NodeID("processor1")]
		p2 := tb.GetGraph().Nodes[NodeID("processor2")]
		assert.Equal(t, []NodeID{NodeID("processor2")}, p1.Children)
		assert.Equal(t, []NodeID{NodeID("processor1")}, p2.Parents)
	})
}

func TestAddSinkNode(t *testing.T) {
	t.Run("valid sink registration", func(t *testing.T) {
		tb := NewBuilder()
		assert.NoError(t, registerTestSource(tb, "source", "input"))

		err := registerTestSink(tb, "sink1", "output", "source")
		assert.NoError(t, err)

		// Verify sink was registered
		node, exists := tb.GetGraph().Nodes[NodeID("sink1")]
		assert.True(t, exists)
		assert.Equal(t, NodeTypeSink, node.Type)

		// Verify parent-child relationship
		sourceNode := tb.GetGraph().Nodes[NodeID("source")]
		assert.Equal(t, []NodeID{NodeID("sink1")}, sourceNode.Children)
	})

	t.Run("parent not found", func(t *testing.T) {
		tb := NewBuilder()

		err := registerTestSink(tb, "sink1", "output", "nonexistent")
		assert.Error(t, err)
		assert.True(t, errors.Is(err, ErrNodeNotFound))
	})

	t.Run("duplicate sink name", func(t *testing.T) {
		tb := NewBuilder()
		assert.NoError(t, registerTestSource(tb, "source", "input"))

		err := registerTestSink(tb, "sink1", "output", "source")
		assert.NoError(t, err)

		// Try to register again - will fail
		err = registerTestSink(tb, "sink1", "output2", "source")
		assert.Error(t, err)
	})

	t.Run("sink after processor", func(t *testing.T) {
		tb := NewBuilder()
		assert.NoError(t, registerTestSource(tb, "source", "input"))
		assert.NoError(t, registerTestProcessor(tb, "processor1", "source"))

		err := registerTestSink(tb, "sink1", "output", "processor1")
		assert.NoError(t, err)

		// Verify
		procNode := tb.GetGraph().Nodes[NodeID("processor1")]
		assert.Equal(t, []NodeID{NodeID("sink1")}, procNode.Children)
	})
}

func TestBuild(t *testing.T) {
	t.Run("valid topology builds successfully", func(t *testing.T) {
		tb := NewBuilder()
		assert.NoError(t, registerTestSource(tb, "source", "input"))
		assert.NoError(t, registerTestProcessor(tb, "processor1", "source"))
		assert.NoError(t, registerTestSink(tb, "sink", "output", "processor1"))

		topology, err := tb.Build()
		assert.NoError(t, err)
		assert.NotZero(t, topology)
		assert.NotZero(t, topology.graph)
		assert.NotZero(t, topology.partitionGroups)
	})

	t.Run("build fails on cycle", func(t *testing.T) {
		tb := NewBuilder()
		assert.NoError(t, registerTestSource(tb, "source", "input"))
		assert.NoError(t, registerTestProcessor(tb, "processor1", "source"))

		// Manually create a cycle in the graph (source -> processor1 -> source)
		tb.GetGraph().Nodes[NodeID("processor1")].Children = []NodeID{NodeID("source")}
		tb.GetGraph().Nodes[NodeID("source")].Parents = []NodeID{NodeID("processor1")}

		_, err := tb.Build()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "validation failed")
	})

	t.Run("build fails on orphaned nodes", func(t *testing.T) {
		tb := NewBuilder()
		assert.NoError(t, registerTestSource(tb, "source", "input"))

		// Manually add orphaned node (not reachable from any source)
		orphan := &Node{
			ID:       NodeID("orphan"),
			Type:     NodeTypeProcessor,
			Children: []NodeID{},
			Parents:  []NodeID{},
		}
		tb.GetGraph().Nodes[NodeID("orphan")] = orphan

		_, err := tb.Build()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "validation failed")
	})

	t.Run("build fails on sink with children", func(t *testing.T) {
		tb := NewBuilder()
		assert.NoError(t, registerTestSource(tb, "source", "input"))
		assert.NoError(t, registerTestSink(tb, "sink", "output", "source"))

		// Manually add a child processor to the sink (invalid!)
		assert.NoError(t, registerTestProcessor(tb, "processor_after_sink", "source"))

		// Now manually connect sink -> processor_after_sink (which is invalid)
		tb.GetGraph().Nodes[NodeID("sink")].Children = []NodeID{NodeID("processor_after_sink")}

		_, err := tb.Build()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "validation failed")
	})

	t.Run("build fails on missing store", func(t *testing.T) {
		tb := NewBuilder()
		assert.NoError(t, registerTestSource(tb, "source", "input"))

		// Manually create processor node with missing store reference
		graphNode := &Node{
			ID:         NodeID("processor1"),
			Type:       NodeTypeProcessor,
			StoreNames: []string{"missing_store"}, // This store doesn't exist!
			Children:   []NodeID{},
			Parents:    []NodeID{NodeID("source")},
		}
		tb.GetGraph().Nodes[NodeID("processor1")] = graphNode
		tb.GetGraph().Nodes[NodeID("source")].Children = []NodeID{NodeID("processor1")}

		_, err := tb.Build()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "validation failed")
	})

	t.Run("build computes partition groups", func(t *testing.T) {
		tb := NewBuilder()
		assert.NoError(t, registerTestSource(tb, "source1", "topic1"))
		assert.NoError(t, registerTestSource(tb, "source2", "topic2"))
		assert.NoError(t, registerTestSink(tb, "sink1", "output1", "source1"))
		assert.NoError(t, registerTestSink(tb, "sink2", "output2", "source2"))

		topology, err := tb.Build()
		assert.NoError(t, err)

		// Should have 2 partition groups (one per source)
		assert.Equal(t, 2, len(topology.partitionGroups))
	})

	t.Run("empty topology builds successfully", func(t *testing.T) {
		tb := NewBuilder()

		topology, err := tb.Build()
		assert.NoError(t, err)
		assert.NotZero(t, topology)
	})
}

func TestMustBuild(t *testing.T) {
	t.Run("valid topology does not panic", func(t *testing.T) {
		tb := NewBuilder()
		assert.NoError(t, registerTestSource(tb, "source", "input"))
		assert.NoError(t, registerTestSink(tb, "sink", "output", "source"))

		topology := tb.MustBuild()
		assert.NotZero(t, topology)
	})

	t.Run("invalid topology panics", func(t *testing.T) {
		tb := NewBuilder()
		assert.NoError(t, registerTestSource(tb, "source", "input"))

		// Create cycle
		tb.GetGraph().Nodes[NodeID("source")].Children = []NodeID{NodeID("source")}

		defer func() {
			if r := recover(); r == nil {
				t.Error("Expected panic but got none")
			}
		}()
		tb.MustBuild()
	})
}

func TestComplexTopologies(t *testing.T) {
	t.Run("branching topology", func(t *testing.T) {
		tb := NewBuilder()
		assert.NoError(t, registerTestSource(tb, "source", "input"))

		// Branch 1: source -> processor1 -> sink1
		assert.NoError(t, registerTestProcessor(tb, "processor1", "source"))
		assert.NoError(t, registerTestSink(tb, "sink1", "output1", "processor1"))

		// Branch 2: source -> processor2 -> sink2
		assert.NoError(t, registerTestProcessor(tb, "processor2", "source"))
		assert.NoError(t, registerTestSink(tb, "sink2", "output2", "processor2"))

		topology, err := tb.Build()
		assert.NoError(t, err)

		// Verify source has 2 children
		sourceNode := topology.graph.Nodes[NodeID("source")]
		assert.Equal(t, 2, len(sourceNode.Children))
	})

	t.Run("merging topology", func(t *testing.T) {
		tb := NewBuilder()
		assert.NoError(t, registerTestSource(tb, "source1", "topic1"))
		assert.NoError(t, registerTestSource(tb, "source2", "topic2"))

		// Both sources feed into the same processor (not typical, but allowed)
		assert.NoError(t, registerTestProcessor(tb, "processor1", "source1"))

		// Manually connect source2 -> processor1
		tb.GetGraph().Nodes[NodeID("source2")].Children = []NodeID{NodeID("processor1")}
		tb.GetGraph().Nodes[NodeID("processor1")].Parents = append(
			tb.GetGraph().Nodes[NodeID("processor1")].Parents,
			NodeID("source2"),
		)

		assert.NoError(t, registerTestSink(tb, "sink", "output", "processor1"))

		topology, err := tb.Build()
		assert.NoError(t, err)

		// Verify processor has 2 parents
		procNode := topology.graph.Nodes[NodeID("processor1")]
		assert.Equal(t, 2, len(procNode.Parents))
	})

	t.Run("deep chain", func(t *testing.T) {
		tb := NewBuilder()
		assert.NoError(t, registerTestSource(tb, "source", "input"))

		// Create a chain of 5 processors
		parent := "source"
		for i := 1; i <= 5; i++ {
			name := fmt.Sprintf("processor%d", i)
			assert.NoError(t, registerTestProcessor(tb, name, parent))
			parent = name
		}

		assert.NoError(t, registerTestSink(tb, "sink", "output", parent))

		topology, err := tb.Build()
		assert.NoError(t, err)

		// Verify chain length: 1 source + 5 processors + 1 sink = 7 nodes
		assert.Equal(t, 7, len(topology.graph.Nodes))
	})
}
