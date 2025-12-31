package kstreams

import (
	"context"
	"testing"

	"github.com/alecthomas/assert/v2"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Test serializers to avoid import cycle with serde package
var testStringDeser = func(data []byte) (string, error) {
	return string(data), nil
}

var testStringSer = func(data string) ([]byte, error) {
	return []byte(data), nil
}

// Test helpers

func testStore(name string, p int32) (Store, error) {
	return &mockStoreForDAG{name: name, partition: p}, nil
}

type mockStoreForDAG struct {
	name      string
	partition int32
}

func (m *mockStoreForDAG) Init() error              { return nil }
func (m *mockStoreForDAG) Flush() error             { return nil }
func (m *mockStoreForDAG) Close() error             { return nil }
func (m *mockStoreForDAG) Checkpoint(ctx context.Context, id string) error { return nil }

func TestBuildTask(t *testing.T) {
	t.Run("simple source-processor topology", func(t *testing.T) {
		tb := NewTopologyBuilder()
		RegisterSource(tb, "source", "input", testStringDeser, testStringDeser)
		RegisterProcessor(tb, func() Processor[string, string, string, string] {
			return &TestProcessor{}
		}, "processor", "source")

		topology, err := tb.Build()
		assert.NoError(t, err)

		// Build task
		task, err := topology.graph.BuildTask([]string{"input"}, 0, nil)
		assert.NoError(t, err)
		assert.NotZero(t, task)

		// Verify task structure
		assert.Equal(t, []string{"input"}, task.topics)
		assert.Equal(t, int32(0), task.partition)
		assert.Equal(t, 1, len(task.rootNodes), "should have 1 source")
		assert.Equal(t, 1, len(task.processors), "should have 1 processor")
		assert.Equal(t, 0, len(task.sinks), "should have no sinks")
		assert.Equal(t, 0, len(task.stores), "should have no stores")
	})

	t.Run("source-processor-sink topology", func(t *testing.T) {
		tb := NewTopologyBuilder()
		RegisterSource(tb, "source", "input", testStringDeser, testStringDeser)
		RegisterProcessor(tb, func() Processor[string, string, string, string] {
			return &TestProcessor{}
		}, "processor", "source")
		RegisterSink(tb, "sink", "output", testStringSer, testStringSer, "processor")

		topology, err := tb.Build()
		assert.NoError(t, err)

		task, err := topology.graph.BuildTask([]string{"input"}, 0, nil)
		assert.NoError(t, err)

		assert.Equal(t, 1, len(task.rootNodes), "should have 1 source")
		assert.Equal(t, 1, len(task.processors), "should have 1 processor")
		assert.Equal(t, 1, len(task.sinks), "should have 1 sink")
	})

	t.Run("topology with state store", func(t *testing.T) {
		tb := NewTopologyBuilder()
		RegisterSource(tb, "source", "input", testStringDeser, testStringDeser)
		RegisterStore(tb, testStore, "test-store")
		RegisterProcessor(tb, func() Processor[string, string, string, string] {
			return &TestProcessor{}
		}, "processor", "source", "test-store")

		topology, err := tb.Build()
		assert.NoError(t, err)

		task, err := topology.graph.BuildTask([]string{"input"}, 0, nil)
		assert.NoError(t, err)

		// Verify store was built
		assert.Equal(t, 1, len(task.stores), "should have 1 store")
		assert.NotZero(t, task.stores["test-store"])

		// Verify processor-to-store mapping
		assert.Equal(t, 1, len(task.processorsToStores), "should have processor-to-store mapping")
		assert.Equal(t, []string{"test-store"}, task.processorsToStores["processor"])
	})

	t.Run("multiple stores", func(t *testing.T) {
		tb := NewTopologyBuilder()
		RegisterSource(tb, "source", "input", testStringDeser, testStringDeser)
		RegisterStore(tb, testStore, "store1")
		RegisterStore(tb, testStore, "store2")
		RegisterProcessor(tb, func() Processor[string, string, string, string] {
			return &TestProcessor{}
		}, "processor1", "source", "store1")
		RegisterProcessor(tb, func() Processor[string, string, string, string] {
			return &TestProcessor{}
		}, "processor2", "processor1", "store2")

		topology, err := tb.Build()
		assert.NoError(t, err)

		task, err := topology.graph.BuildTask([]string{"input"}, 0, nil)
		assert.NoError(t, err)

		// Verify both stores were built
		assert.Equal(t, 2, len(task.stores))
		assert.NotZero(t, task.stores["store1"])
		assert.NotZero(t, task.stores["store2"])

		// Verify processor-to-store mappings
		assert.Equal(t, []string{"store1"}, task.processorsToStores["processor1"])
		assert.Equal(t, []string{"store2"}, task.processorsToStores["processor2"])
	})

	t.Run("multiple sources", func(t *testing.T) {
		tb := NewTopologyBuilder()
		RegisterSource(tb, "source1", "topic1", testStringDeser, testStringDeser)
		RegisterSource(tb, "source2", "topic2", testStringDeser, testStringDeser)
		RegisterProcessor(tb, func() Processor[string, string, string, string] {
			return &TestProcessor{}
		}, "processor", "source1")
		SetParent(tb, "source2", "processor")

		topology, err := tb.Build()
		assert.NoError(t, err)

		task, err := topology.graph.BuildTask([]string{"topic1", "topic2"}, 0, nil)
		assert.NoError(t, err)

		// Verify both sources are present
		assert.Equal(t, 2, len(task.rootNodes), "should have 2 sources")
		assert.NotZero(t, task.rootNodes["topic1"])
		assert.NotZero(t, task.rootNodes["topic2"])
	})

	t.Run("branching topology", func(t *testing.T) {
		tb := NewTopologyBuilder()
		RegisterSource(tb, "source", "input", testStringDeser, testStringDeser)
		RegisterProcessor(tb, func() Processor[string, string, string, string] {
			return &TestProcessor{}
		}, "processor1", "source")
		RegisterProcessor(tb, func() Processor[string, string, string, string] {
			return &TestProcessor{}
		}, "processor2", "source")

		topology, err := tb.Build()
		assert.NoError(t, err)

		task, err := topology.graph.BuildTask([]string{"input"}, 0, nil)
		assert.NoError(t, err)

		// Verify both branches
		assert.Equal(t, 2, len(task.processors), "should have 2 processors")
		assert.NotZero(t, task.processors["processor1"])
		assert.NotZero(t, task.processors["processor2"])
	})

	t.Run("deep chain", func(t *testing.T) {
		tb := NewTopologyBuilder()
		RegisterSource(tb, "source", "input", testStringDeser, testStringDeser)
		RegisterProcessor(tb, func() Processor[string, string, string, string] {
			return &TestProcessor{}
		}, "p1", "source")
		RegisterProcessor(tb, func() Processor[string, string, string, string] {
			return &TestProcessor{}
		}, "p2", "p1")
		RegisterProcessor(tb, func() Processor[string, string, string, string] {
			return &TestProcessor{}
		}, "p3", "p2")
		RegisterProcessor(tb, func() Processor[string, string, string, string] {
			return &TestProcessor{}
		}, "p4", "p3")
		RegisterSink(tb, "sink", "output", testStringSer, testStringSer, "p4")

		topology, err := tb.Build()
		assert.NoError(t, err)

		task, err := topology.graph.BuildTask([]string{"input"}, 0, nil)
		assert.NoError(t, err)

		// Verify all nodes built
		assert.Equal(t, 1, len(task.rootNodes))
		assert.Equal(t, 4, len(task.processors))
		assert.Equal(t, 1, len(task.sinks))
	})

	t.Run("multiple partitions", func(t *testing.T) {
		tb := NewTopologyBuilder()
		RegisterSource(tb, "source", "input", testStringDeser, testStringDeser)
		RegisterStore(tb, testStore, "store")
		RegisterProcessor(tb, func() Processor[string, string, string, string] {
			return &TestProcessor{}
		}, "processor", "source", "store")

		topology, err := tb.Build()
		assert.NoError(t, err)

		// Build tasks for different partitions
		task0, err := topology.graph.BuildTask([]string{"input"}, 0, nil)
		assert.NoError(t, err)
		task1, err := topology.graph.BuildTask([]string{"input"}, 1, nil)
		assert.NoError(t, err)
		task2, err := topology.graph.BuildTask([]string{"input"}, 2, nil)
		assert.NoError(t, err)

		// Verify each task has correct partition
		assert.Equal(t, int32(0), task0.partition)
		assert.Equal(t, int32(1), task1.partition)
		assert.Equal(t, int32(2), task2.partition)

		// Verify stores are separate instances per partition
		store0 := task0.stores["store"].(*mockStoreForDAG)
		store1 := task1.stores["store"].(*mockStoreForDAG)
		store2 := task2.stores["store"].(*mockStoreForDAG)

		assert.Equal(t, int32(0), store0.partition)
		assert.Equal(t, int32(1), store1.partition)
		assert.Equal(t, int32(2), store2.partition)
	})
}

func TestFindTopicForSource(t *testing.T) {
	t.Run("finds topic for source node", func(t *testing.T) {
		tb := NewTopologyBuilder()
		RegisterSource(tb, "source1", "topic1", testStringDeser, testStringDeser)
		RegisterSource(tb, "source2", "topic2", testStringDeser, testStringDeser)

		topology, err := tb.Build()
		assert.NoError(t, err)

		// Find topics
		topic1 := topology.graph.findTopicForSource(NodeID("source1"))
		topic2 := topology.graph.findTopicForSource(NodeID("source2"))

		assert.Equal(t, "topic1", topic1)
		assert.Equal(t, "topic2", topic2)
	})

	t.Run("returns empty string for non-existent node", func(t *testing.T) {
		tb := NewTopologyBuilder()
		RegisterSource(tb, "source", "topic", testStringDeser, testStringDeser)

		topology, err := tb.Build()
		assert.NoError(t, err)

		topic := topology.graph.findTopicForSource(NodeID("non-existent"))
		assert.Equal(t, "", topic)
	})

	t.Run("returns empty string for processor node", func(t *testing.T) {
		tb := NewTopologyBuilder()
		RegisterSource(tb, "source", "topic", testStringDeser, testStringDeser)
		RegisterProcessor(tb, func() Processor[string, string, string, string] {
			return &TestProcessor{}
		}, "processor", "source")

		topology, err := tb.Build()
		assert.NoError(t, err)

		// Processor node should not have a topic
		topic := topology.graph.findTopicForSource(NodeID("processor"))
		assert.Equal(t, "", topic)
	})
}

func TestExtractRootNodes(t *testing.T) {
	t.Run("extracts root nodes for given topics", func(t *testing.T) {
		sources := map[string]NodeID{
			"topic1": NodeID("source1"),
			"topic2": NodeID("source2"),
			"topic3": NodeID("source3"),
		}

		// Mock built nodes
		mockSource1 := &mockSourceForDAG{}
		mockSource2 := &mockSourceForDAG{}
		mockSource3 := &mockSourceForDAG{}

		builtNodes := map[NodeID]RuntimeNode{
			NodeID("source1"): mockSource1,
			NodeID("source2"): mockSource2,
			NodeID("source3"): mockSource3,
		}

		// Extract subset of topics
		topics := []string{"topic1", "topic3"}
		rootNodes := extractRootNodes(topics, sources, builtNodes)

		assert.Equal(t, 2, len(rootNodes))
		assert.NotZero(t, rootNodes["topic1"])
		assert.NotZero(t, rootNodes["topic3"])
	})

	t.Run("handles empty topics", func(t *testing.T) {
		sources := map[string]NodeID{
			"topic1": NodeID("source1"),
		}
		builtNodes := map[NodeID]RuntimeNode{
			NodeID("source1"): &mockSourceForDAG{},
		}

		rootNodes := extractRootNodes([]string{}, sources, builtNodes)
		assert.Equal(t, 0, len(rootNodes))
	})

	t.Run("skips non-existent topics", func(t *testing.T) {
		sources := map[string]NodeID{
			"topic1": NodeID("source1"),
		}
		builtNodes := map[NodeID]RuntimeNode{
			NodeID("source1"): &mockSourceForDAG{},
		}

		topics := []string{"topic1", "non-existent"}
		rootNodes := extractRootNodes(topics, sources, builtNodes)

		assert.Equal(t, 1, len(rootNodes))
		assert.NotZero(t, rootNodes["topic1"])
	})

	t.Run("skips nodes that are not RawRecordProcessor", func(t *testing.T) {
		sources := map[string]NodeID{
			"topic1": NodeID("source1"),
		}

		// Node that doesn't implement RawRecordProcessor
		mockNode := &mockNodeNotRawProcessor{}

		builtNodes := map[NodeID]RuntimeNode{
			NodeID("source1"): mockNode,
		}

		topics := []string{"topic1"}
		rootNodes := extractRootNodes(topics, sources, builtNodes)

		// Should be empty because mockNode is not a RawRecordProcessor
		assert.Equal(t, 0, len(rootNodes))
	})
}

func TestBuildTaskComplex(t *testing.T) {
	t.Run("complex multi-branch topology builds correctly", func(t *testing.T) {
		tb := NewTopologyBuilder()
		RegisterSource(tb, "source", "input", testStringDeser, testStringDeser)

		// Create multiple branches
		RegisterProcessor(tb, func() Processor[string, string, string, string] {
			return &TestProcessor{}
		}, "branch1", "source")
		RegisterProcessor(tb, func() Processor[string, string, string, string] {
			return &TestProcessor{}
		}, "branch2", "source")

		// Each branch has children
		RegisterProcessor(tb, func() Processor[string, string, string, string] {
			return &TestProcessor{}
		}, "branch1-child", "branch1")
		RegisterProcessor(tb, func() Processor[string, string, string, string] {
			return &TestProcessor{}
		}, "branch2-child", "branch2")

		topology, err := tb.Build()
		assert.NoError(t, err)

		task, err := topology.graph.BuildTask([]string{"input"}, 0, nil)
		assert.NoError(t, err)

		// Verify all processors were built
		assert.Equal(t, 4, len(task.processors), "should have 4 processors")
		assert.NotZero(t, task.processors["branch1"])
		assert.NotZero(t, task.processors["branch2"])
		assert.NotZero(t, task.processors["branch1-child"])
		assert.NotZero(t, task.processors["branch2-child"])
	})
}

// Test mocks and helpers

type mockSourceForDAG struct{}

func (m *mockSourceForDAG) Init() error  { return nil }
func (m *mockSourceForDAG) Process(ctx context.Context, record *kgo.Record) error {
	return nil
}
func (m *mockSourceForDAG) Close() error { return nil }

type mockNodeNotRawProcessor struct{}

func (m *mockNodeNotRawProcessor) Init() error  { return nil }
func (m *mockNodeNotRawProcessor) Close() error { return nil }
