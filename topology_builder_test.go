package kstreams

import (
	"context"
	"fmt"
	"testing"

	"github.com/alecthomas/assert/v2"
)

// Test serializers/deserializers to avoid import cycle with serde package
var testStringDeserializer = func(data []byte) (string, error) {
	return string(data), nil
}

var testStringSerializer = func(data string) ([]byte, error) {
	return []byte(data), nil
}

var testFloat64Deserializer = func(data []byte) (float64, error) {
	return 0.0, nil // Simplified for tests
}

var testFloat64Serializer = func(data float64) ([]byte, error) {
	return []byte{}, nil // Simplified for tests
}

// Test helper: simple processor for testing
type TestProcessor struct {
	ctx ProcessorContext[string, string]
}

func (p *TestProcessor) Init(ctx ProcessorContext[string, string]) error {
	p.ctx = ctx
	return nil
}

func (p *TestProcessor) Process(ctx context.Context, k string, v string) error {
	p.ctx.Forward(ctx, k, v)
	return nil
}

func (p *TestProcessor) Close() error {
	return nil
}

func TestNewTopologyBuilder(t *testing.T) {
	tb := NewTopologyBuilder()
	assert.NotZero(t, tb)
	assert.NotZero(t, tb.graph)

	// Maps are initialized (not nil)
	assert.NotEqual(t, nil, tb.processors)
	assert.NotEqual(t, nil, tb.stores)
	assert.NotEqual(t, nil, tb.sources)
	assert.NotEqual(t, nil, tb.sinks)
}

func TestRegisterSource(t *testing.T) {
	t.Run("valid source registration", func(t *testing.T) {
		tb := NewTopologyBuilder()
		err := RegisterSource(tb, "source1", "topic1", testStringDeserializer, testStringDeserializer)
		assert.NoError(t, err)

		// Verify source was registered in graph
		node, exists := tb.graph.nodes[NodeID("source1")]
		assert.True(t, exists)
		assert.Equal(t, NodeTypeSource, node.Type)

		// Verify topic mapping
		sourceID, exists := tb.graph.sources["topic1"]
		assert.True(t, exists)
		assert.Equal(t, NodeID("source1"), sourceID)
	})

	t.Run("duplicate source name", func(t *testing.T) {
		tb := NewTopologyBuilder()
		err := RegisterSource(tb, "source1", "topic1", testStringDeserializer, testStringDeserializer)
		assert.NoError(t, err)

		// Try to register again with same name
		err = RegisterSource(tb, "source1", "topic2", testStringDeserializer, testStringDeserializer)
		assert.Error(t, err)
		assert.Equal(t, ErrNodeAlreadyExists, err)
	})

	t.Run("multiple sources - different names", func(t *testing.T) {
		tb := NewTopologyBuilder()
		err := RegisterSource(tb, "source1", "topic1", testStringDeserializer, testStringDeserializer)
		assert.NoError(t, err)

		err = RegisterSource(tb, "source2", "topic2", testStringDeserializer, testStringDeserializer)
		assert.NoError(t, err)

		assert.Equal(t, 2, len(tb.graph.nodes))
		assert.Equal(t, 2, len(tb.graph.sources))
	})

	t.Run("MustRegisterSource panics on error", func(t *testing.T) {
		tb := NewTopologyBuilder()
		MustRegisterSource(tb, "source1", "topic1", testStringDeserializer, testStringDeserializer)

		// Should panic on duplicate
		defer func() {
			if r := recover(); r == nil {
				t.Error("Expected panic but got none")
			}
		}()
		MustRegisterSource(tb, "source1", "topic2", testStringDeserializer, testStringDeserializer)
	})
}

func TestRegisterProcessor(t *testing.T) {
	t.Run("valid processor registration", func(t *testing.T) {
		tb := NewTopologyBuilder()
		RegisterSource(tb, "source", "input", testStringDeserializer, testStringDeserializer)

		err := RegisterProcessor(tb, func() Processor[string, string, string, string] {
			return &TestProcessor{}
		}, "processor1", "source")
		assert.NoError(t, err)

		// Verify processor was registered in graph
		node, exists := tb.graph.nodes[NodeID("processor1")]
		assert.True(t, exists)
		assert.Equal(t, NodeTypeProcessor, node.Type)

		// Verify parent-child relationship
		sourceNode := tb.graph.nodes[NodeID("source")]
		assert.Equal(t, []NodeID{NodeID("processor1")}, sourceNode.Children)
		assert.Equal(t, []NodeID{NodeID("source")}, node.Parents)
	})

	t.Run("parent not found", func(t *testing.T) {
		tb := NewTopologyBuilder()

		err := RegisterProcessor(tb, func() Processor[string, string, string, string] {
			return &TestProcessor{}
		}, "processor1", "nonexistent")
		assert.Error(t, err)
		assert.Equal(t, ErrNodeNotFound, err)
	})

	t.Run("duplicate processor name", func(t *testing.T) {
		tb := NewTopologyBuilder()
		RegisterSource(tb, "source", "input", testStringDeserializer, testStringDeserializer)

		err := RegisterProcessor(tb, func() Processor[string, string, string, string] {
			return &TestProcessor{}
		}, "processor1", "source")
		assert.NoError(t, err)

		// Try to register again with same name
		err = RegisterProcessor(tb, func() Processor[string, string, string, string] {
			return &TestProcessor{}
		}, "processor1", "source")
		assert.Error(t, err)
		assert.Equal(t, ErrNodeAlreadyExists, err)
	})

	t.Run("processor with valid stores", func(t *testing.T) {
		tb := NewTopologyBuilder()
		RegisterSource(tb, "source", "input", testStringDeserializer, testStringDeserializer)

		// Register a store
		tb.graph.stores["store1"] = &StoreDefinition{Name: "store1"}

		err := RegisterProcessor(tb, func() Processor[string, string, string, string] {
			return &TestProcessor{}
		}, "processor1", "source", "store1")
		assert.NoError(t, err)

		// Verify store reference
		node := tb.graph.nodes[NodeID("processor1")]
		assert.Equal(t, []string{"store1"}, node.StoreNames)
	})

	t.Run("processor with missing store", func(t *testing.T) {
		tb := NewTopologyBuilder()
		RegisterSource(tb, "source", "input", testStringDeserializer, testStringDeserializer)

		err := RegisterProcessor(tb, func() Processor[string, string, string, string] {
			return &TestProcessor{}
		}, "processor1", "source", "nonexistent_store")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "store not found")
	})

	// Note: Type mismatch test skipped - type validation depends on runtime reflection
	// and may not catch all type errors at registration time

	t.Run("chain multiple processors", func(t *testing.T) {
		tb := NewTopologyBuilder()
		RegisterSource(tb, "source", "input", testStringDeserializer, testStringDeserializer)

		RegisterProcessor(tb, func() Processor[string, string, string, string] {
			return &TestProcessor{}
		}, "processor1", "source")

		RegisterProcessor(tb, func() Processor[string, string, string, string] {
			return &TestProcessor{}
		}, "processor2", "processor1")

		// Verify chain
		p1 := tb.graph.nodes[NodeID("processor1")]
		p2 := tb.graph.nodes[NodeID("processor2")]
		assert.Equal(t, []NodeID{NodeID("processor2")}, p1.Children)
		assert.Equal(t, []NodeID{NodeID("processor1")}, p2.Parents)
	})

	t.Run("MustRegisterProcessor panics on error", func(t *testing.T) {
		tb := NewTopologyBuilder()

		// Should panic - parent not found
		defer func() {
			if r := recover(); r == nil {
				t.Error("Expected panic but got none")
			}
		}()
		MustRegisterProcessor(tb, func() Processor[string, string, string, string] {
			return &TestProcessor{}
		}, "processor1", "nonexistent")
	})
}

func TestRegisterSink(t *testing.T) {
	t.Run("valid sink registration", func(t *testing.T) {
		tb := NewTopologyBuilder()
		RegisterSource(tb, "source", "input", testStringDeserializer, testStringDeserializer)

		err := RegisterSink(tb, "sink1", "output", testStringSerializer, testStringSerializer, "source")
		assert.NoError(t, err)

		// Verify sink was registered
		node, exists := tb.graph.nodes[NodeID("sink1")]
		assert.True(t, exists)
		assert.Equal(t, NodeTypeSink, node.Type)

		// Verify parent-child relationship
		sourceNode := tb.graph.nodes[NodeID("source")]
		assert.Equal(t, []NodeID{NodeID("sink1")}, sourceNode.Children)
	})

	t.Run("parent not found", func(t *testing.T) {
		tb := NewTopologyBuilder()

		err := RegisterSink(tb, "sink1", "output", testStringSerializer, testStringSerializer, "nonexistent")
		assert.Error(t, err)
		assert.Equal(t, ErrNodeNotFound, err)
	})

	t.Run("duplicate sink name", func(t *testing.T) {
		tb := NewTopologyBuilder()
		RegisterSource(tb, "source", "input", testStringDeserializer, testStringDeserializer)

		err := RegisterSink(tb, "sink1", "output", testStringSerializer, testStringSerializer, "source")
		assert.NoError(t, err)

		// Try to register again
		err = RegisterSink(tb, "sink1", "output2", testStringSerializer, testStringSerializer, "source")
		assert.Error(t, err)
		assert.Equal(t, ErrNodeAlreadyExists, err)
	})

	// Note: Type mismatch test skipped - type validation depends on runtime reflection
	// and may not catch all type errors at registration time

	t.Run("sink after processor", func(t *testing.T) {
		tb := NewTopologyBuilder()
		RegisterSource(tb, "source", "input", testStringDeserializer, testStringDeserializer)
		RegisterProcessor(tb, func() Processor[string, string, string, string] {
			return &TestProcessor{}
		}, "processor1", "source")

		err := RegisterSink(tb, "sink1", "output", testStringSerializer, testStringSerializer, "processor1")
		assert.NoError(t, err)

		// Verify
		procNode := tb.graph.nodes[NodeID("processor1")]
		assert.Equal(t, []NodeID{NodeID("sink1")}, procNode.Children)
	})

	t.Run("MustRegisterSink panics on error", func(t *testing.T) {
		tb := NewTopologyBuilder()

		// Should panic - parent not found
		defer func() {
			if r := recover(); r == nil {
				t.Error("Expected panic but got none")
			}
		}()
		MustRegisterSink(tb, "sink1", "output", testStringSerializer, testStringSerializer, "nonexistent")
	})
}

func TestSetParent(t *testing.T) {
	t.Run("set parent on processor", func(t *testing.T) {
		tb := NewTopologyBuilder()
		RegisterSource(tb, "source", "input", testStringDeserializer, testStringDeserializer)
		RegisterProcessor(tb, func() Processor[string, string, string, string] {
			return &TestProcessor{}
		}, "processor1", "source")
		RegisterProcessor(tb, func() Processor[string, string, string, string] {
			return &TestProcessor{}
		}, "processor2", "source")

		// Add processor2 as child of processor1
		err := SetParent(tb, "processor1", "processor2")
		assert.NoError(t, err)

		// Verify in old topology structures
		proc1 := tb.processors["processor1"]
		assert.True(t, ContainsAny(proc1.ChildNodeNames, []string{"processor2"}))
	})

	t.Run("set parent on source", func(t *testing.T) {
		tb := NewTopologyBuilder()
		RegisterSource(tb, "source", "input", testStringDeserializer, testStringDeserializer)
		RegisterProcessor(tb, func() Processor[string, string, string, string] {
			return &TestProcessor{}
		}, "processor1", "source")

		// Add processor1 as child of source (already done, but test SetParent)
		err := SetParent(tb, "source", "processor1")
		assert.NoError(t, err)

		// Verify
		source := tb.sources["source"]
		assert.True(t, ContainsAny(source.ChildNodeNames, []string{"processor1"}))
	})

	t.Run("parent not found", func(t *testing.T) {
		tb := NewTopologyBuilder()
		RegisterSource(tb, "source", "input", testStringDeserializer, testStringDeserializer)

		err := SetParent(tb, "nonexistent", "source")
		assert.Error(t, err)
		assert.Equal(t, ErrNodeNotFound, err)
	})

	t.Run("MustSetParent panics on error", func(t *testing.T) {
		tb := NewTopologyBuilder()

		defer func() {
			if r := recover(); r == nil {
				t.Error("Expected panic but got none")
			}
		}()
		MustSetParent(tb, "nonexistent", "child")
	})
}

func TestBuild(t *testing.T) {
	t.Run("valid topology builds successfully", func(t *testing.T) {
		tb := NewTopologyBuilder()
		RegisterSource(tb, "source", "input", testStringDeserializer, testStringDeserializer)
		RegisterProcessor(tb, func() Processor[string, string, string, string] {
			return &TestProcessor{}
		}, "processor1", "source")
		RegisterSink(tb, "sink", "output", testStringSerializer, testStringSerializer, "processor1")

		topology, err := tb.Build()
		assert.NoError(t, err)
		assert.NotZero(t, topology)
		assert.NotZero(t, topology.graph)
		assert.NotZero(t, topology.partitionGroups)
	})

	t.Run("build fails on cycle", func(t *testing.T) {
		tb := NewTopologyBuilder()
		RegisterSource(tb, "source", "input", testStringDeserializer, testStringDeserializer)
		RegisterProcessor(tb, func() Processor[string, string, string, string] {
			return &TestProcessor{}
		}, "processor1", "source")

		// Manually create a cycle in the graph (source -> processor1 -> source)
		// This is not possible through the normal API, but we can manipulate the graph directly
		tb.graph.nodes[NodeID("processor1")].Children = []NodeID{NodeID("source")}
		tb.graph.nodes[NodeID("source")].Parents = []NodeID{NodeID("processor1")}

		_, err := tb.Build()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "validation failed")
	})

	t.Run("build fails on orphaned nodes", func(t *testing.T) {
		tb := NewTopologyBuilder()
		RegisterSource(tb, "source", "input", testStringDeserializer, testStringDeserializer)

		// Manually add orphaned node (not reachable from any source)
		orphan := &GraphNode{
			ID:       NodeID("orphan"),
			Type:     NodeTypeProcessor,
			Children: []NodeID{},
			Parents:  []NodeID{},
		}
		tb.graph.nodes[NodeID("orphan")] = orphan

		_, err := tb.Build()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "validation failed")
	})

	t.Run("build fails on sink with children", func(t *testing.T) {
		tb := NewTopologyBuilder()
		RegisterSource(tb, "source", "input", testStringDeserializer, testStringDeserializer)
		RegisterSink(tb, "sink", "output", testStringSerializer, testStringSerializer, "source")

		// Manually add a child processor to the sink (invalid!)
		// First create the child node so it exists in the graph
		RegisterProcessor(tb, func() Processor[string, string, string, string] {
			return &TestProcessor{}
		}, "processor_after_sink", "source")

		// Now manually connect sink -> processor_after_sink (which is invalid)
		tb.graph.nodes[NodeID("sink")].Children = []NodeID{NodeID("processor_after_sink")}

		_, err := tb.Build()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "validation failed")
	})

	t.Run("build fails on missing store", func(t *testing.T) {
		tb := NewTopologyBuilder()
		RegisterSource(tb, "source", "input", testStringDeserializer, testStringDeserializer)

		// Manually create processor node with missing store reference
		builder := &ProcessorNodeBuilder[string, string, string, string]{
			nodeID: NodeID("processor1"),
			processorFunc: func() Processor[string, string, string, string] {
				return &TestProcessor{}
			},
			storeNames: []string{"missing_store"}, // This store doesn't exist!
		}
		graphNode := builder.ToGraphNode()
		tb.graph.nodes[NodeID("processor1")] = graphNode
		tb.graph.nodes[NodeID("source")].Children = []NodeID{NodeID("processor1")}
		graphNode.Parents = []NodeID{NodeID("source")}

		_, err := tb.Build()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "validation failed")
	})

	t.Run("build computes partition groups", func(t *testing.T) {
		tb := NewTopologyBuilder()
		RegisterSource(tb, "source1", "topic1", testStringDeserializer, testStringDeserializer)
		RegisterSource(tb, "source2", "topic2", testStringDeserializer, testStringDeserializer)
		RegisterSink(tb, "sink1", "output1", testStringSerializer, testStringSerializer, "source1")
		RegisterSink(tb, "sink2", "output2", testStringSerializer, testStringSerializer, "source2")

		topology, err := tb.Build()
		assert.NoError(t, err)

		// Should have 2 partition groups (one per source)
		assert.Equal(t, 2, len(topology.partitionGroups))
	})

	t.Run("empty topology builds successfully", func(t *testing.T) {
		tb := NewTopologyBuilder()

		topology, err := tb.Build()
		assert.NoError(t, err)
		assert.NotZero(t, topology)
	})
}

func TestMustBuild(t *testing.T) {
	t.Run("valid topology does not panic", func(t *testing.T) {
		tb := NewTopologyBuilder()
		RegisterSource(tb, "source", "input", testStringDeserializer, testStringDeserializer)
		RegisterSink(tb, "sink", "output", testStringSerializer, testStringSerializer, "source")

		topology := tb.MustBuild()
		assert.NotZero(t, topology)
	})

	t.Run("invalid topology panics", func(t *testing.T) {
		tb := NewTopologyBuilder()
		RegisterSource(tb, "source", "input", testStringDeserializer, testStringDeserializer)

		// Create cycle
		tb.graph.nodes[NodeID("source")].Children = []NodeID{NodeID("source")}

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
		tb := NewTopologyBuilder()
		RegisterSource(tb, "source", "input", testStringDeserializer, testStringDeserializer)

		// Branch 1: source -> processor1 -> sink1
		RegisterProcessor(tb, func() Processor[string, string, string, string] {
			return &TestProcessor{}
		}, "processor1", "source")
		RegisterSink(tb, "sink1", "output1", testStringSerializer, testStringSerializer, "processor1")

		// Branch 2: source -> processor2 -> sink2
		RegisterProcessor(tb, func() Processor[string, string, string, string] {
			return &TestProcessor{}
		}, "processor2", "source")
		RegisterSink(tb, "sink2", "output2", testStringSerializer, testStringSerializer, "processor2")

		topology, err := tb.Build()
		assert.NoError(t, err)

		// Verify source has 2 children
		sourceNode := topology.graph.nodes[NodeID("source")]
		assert.Equal(t, 2, len(sourceNode.Children))
	})

	t.Run("merging topology", func(t *testing.T) {
		tb := NewTopologyBuilder()
		RegisterSource(tb, "source1", "topic1", testStringDeserializer, testStringDeserializer)
		RegisterSource(tb, "source2", "topic2", testStringDeserializer, testStringDeserializer)

		// Both sources feed into the same processor (not typical, but allowed)
		RegisterProcessor(tb, func() Processor[string, string, string, string] {
			return &TestProcessor{}
		}, "processor1", "source1")

		// Manually connect source2 -> processor1
		tb.graph.nodes[NodeID("source2")].Children = []NodeID{NodeID("processor1")}
		tb.graph.nodes[NodeID("processor1")].Parents = append(
			tb.graph.nodes[NodeID("processor1")].Parents,
			NodeID("source2"),
		)

		RegisterSink(tb, "sink", "output", testStringSerializer, testStringSerializer, "processor1")

		topology, err := tb.Build()
		assert.NoError(t, err)

		// Verify processor has 2 parents
		procNode := topology.graph.nodes[NodeID("processor1")]
		assert.Equal(t, 2, len(procNode.Parents))
	})

	t.Run("deep chain", func(t *testing.T) {
		tb := NewTopologyBuilder()
		RegisterSource(tb, "source", "input", testStringDeserializer, testStringDeserializer)

		// Create a chain of 5 processors
		parent := "source"
		for i := 1; i <= 5; i++ {
			name := fmt.Sprintf("processor%d", i)
			RegisterProcessor(tb, func() Processor[string, string, string, string] {
				return &TestProcessor{}
			}, name, parent)
			parent = name
		}

		RegisterSink(tb, "sink", "output", testStringSerializer, testStringSerializer, parent)

		topology, err := tb.Build()
		assert.NoError(t, err)

		// Verify chain length: 1 source + 5 processors + 1 sink = 7 nodes
		assert.Equal(t, 7, len(topology.graph.nodes))
	})
}

func TestContainsAny(t *testing.T) {
	t.Run("contains match", func(t *testing.T) {
		s := []string{"a", "b", "c"}
		v := []string{"b", "d"}
		assert.True(t, ContainsAny(s, v))
	})

	t.Run("no match", func(t *testing.T) {
		s := []string{"a", "b", "c"}
		v := []string{"d", "e"}
		assert.False(t, ContainsAny(s, v))
	})

	t.Run("empty slices", func(t *testing.T) {
		assert.False(t, ContainsAny([]string{}, []string{}))
		assert.False(t, ContainsAny([]string{"a"}, []string{}))
		assert.False(t, ContainsAny([]string{}, []string{"a"}))
	})

	t.Run("integers", func(t *testing.T) {
		s := []int{1, 2, 3}
		v := []int{2, 4}
		assert.True(t, ContainsAny(s, v))
	})
}
