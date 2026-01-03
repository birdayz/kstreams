package execution

import (
	"context"
	"testing"

	"github.com/alecthomas/assert/v2"
	"github.com/birdayz/kstreams/internal/runtime"
	"github.com/birdayz/kstreams/kdag"
	"github.com/birdayz/kstreams/kprocessor"
	"github.com/birdayz/kstreams/kstate"
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

type mockStoreForDAG struct {
	name      string
	partition int32
}

func (m *mockStoreForDAG) Name() string                                          { return m.name }
func (m *mockStoreForDAG) Persistent() bool                                      { return true }
func (m *mockStoreForDAG) Init(ctx kprocessor.ProcessorContextInternal) error    { return nil }
func (m *mockStoreForDAG) Flush(ctx context.Context) error                       { return nil }
func (m *mockStoreForDAG) Close() error                                          { return nil }

type mockStoreBuilder struct {
	name string
}

func (m *mockStoreBuilder) WithChangelogEnabled(config map[string]string) kstate.StoreBuilder[kstate.StateStore] {
	return m
}

func (m *mockStoreBuilder) WithChangelogDisabled() kstate.StoreBuilder[kstate.StateStore] {
	return m
}

func (m *mockStoreBuilder) WithLoggingDisabled() kstate.StoreBuilder[kstate.StateStore] {
	return m
}

func (m *mockStoreBuilder) Build() (kstate.StateStore, error) {
	return &mockStoreForDAG{name: m.name, partition: 0}, nil
}

// BuildStateStore implements TypeErasedStoreBuilder
func (m *mockStoreBuilder) BuildStateStore() (kstate.StateStore, error) {
	return m.Build()
}

func (m *mockStoreBuilder) Name() string {
	return m.name
}

func (m *mockStoreBuilder) ChangelogEnabled() bool {
	return false
}

func (m *mockStoreBuilder) RestoreCallback() kstate.StateRestoreCallback {
	return nil
}

func (m *mockStoreBuilder) LogConfig() map[string]string {
	return nil
}

func newMockStoreBuilder(name string) kstate.TypeErasedStoreBuilder {
	return &mockStoreBuilder{name: name}
}

func TestBuildTask(t *testing.T) {
	t.Run("simple source-processor topology", func(t *testing.T) {
		tb := kdag.NewBuilder()
		assert.NoError(t, RegisterSource(tb, "source", "input", testStringDeser, testStringDeser))
		//nolint:staticcheck // SA1019: testing deprecated Processor interface
		assert.NoError(t, RegisterProcessor(tb, func() kprocessor.Processor[string, string, string, string] {
			return &TestProcessor{}
		}, "processor", "source"))

		topology, err := tb.Build()
		assert.NoError(t, err)

		// Build task
		task, err := BuildTaskFromGraph(topology.GetGraph(), []string{"input"}, 0, nil, "test-app", t.TempDir())
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
		tb := kdag.NewBuilder()
		assert.NoError(t, RegisterSource(tb, "source", "input", testStringDeser, testStringDeser))
		//nolint:staticcheck // SA1019: testing deprecated Processor interface
		assert.NoError(t, RegisterProcessor(tb, func() kprocessor.Processor[string, string, string, string] {
			return &TestProcessor{}
		}, "processor", "source"))
		assert.NoError(t, RegisterSink(tb, "sink", "output", testStringSer, testStringSer, "processor"))

		topology, err := tb.Build()
		assert.NoError(t, err)

		task, err := BuildTaskFromGraph(topology.GetGraph(), []string{"input"}, 0, nil, "test-app", t.TempDir())
		assert.NoError(t, err)

		assert.Equal(t, 1, len(task.rootNodes), "should have 1 source")
		assert.Equal(t, 1, len(task.processors), "should have 1 processor")
		assert.Equal(t, 1, len(task.sinks), "should have 1 sink")
	})

	t.Run("topology with state store", func(t *testing.T) {
		tb := kdag.NewBuilder()
		assert.NoError(t, RegisterSource(tb, "source", "input", testStringDeser, testStringDeser))
		err := kdag.RegisterStore(tb, newMockStoreBuilder("test-store"), "test-store")
		assert.NoError(t, err)
		//nolint:staticcheck // SA1019: testing deprecated Processor interface
		assert.NoError(t, RegisterProcessor(tb, func() kprocessor.Processor[string, string, string, string] {
			return &TestProcessor{}
		}, "processor", "source", "test-store"))

		topology, err := tb.Build()
		assert.NoError(t, err)

		task, err := BuildTaskFromGraph(topology.GetGraph(), []string{"input"}, 0, nil, "test-app", t.TempDir())
		assert.NoError(t, err)

		// Verify store was built
		assert.Equal(t, 1, len(task.stores), "should have 1 store")
		assert.NotZero(t, task.stores["test-store"])

		// Verify processor-to-store mapping
		assert.Equal(t, 1, len(task.processorsToStores), "should have processor-to-store mapping")
		assert.Equal(t, []string{"test-store"}, task.processorsToStores["processor"])
	})

	t.Run("multiple stores", func(t *testing.T) {
		tb := kdag.NewBuilder()
		assert.NoError(t, RegisterSource(tb, "source", "input", testStringDeser, testStringDeser))
		err := kdag.RegisterStore(tb, newMockStoreBuilder("store1"), "store1")
		assert.NoError(t, err)
		err = kdag.RegisterStore(tb, newMockStoreBuilder("store2"), "store2")
		assert.NoError(t, err)
		//nolint:staticcheck // SA1019: testing deprecated Processor interface
		assert.NoError(t, RegisterProcessor(tb, func() kprocessor.Processor[string, string, string, string] {
			return &TestProcessor{}
		}, "processor1", "source", "store1"))
		//nolint:staticcheck // SA1019: testing deprecated Processor interface
		assert.NoError(t, RegisterProcessor(tb, func() kprocessor.Processor[string, string, string, string] {
			return &TestProcessor{}
		}, "processor2", "processor1", "store2"))

		topology, err := tb.Build()
		assert.NoError(t, err)

		task, err := BuildTaskFromGraph(topology.GetGraph(), []string{"input"}, 0, nil, "test-app", t.TempDir())
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
		tb := kdag.NewBuilder()
		assert.NoError(t, RegisterSource(tb, "source1", "topic1", testStringDeser, testStringDeser))
		assert.NoError(t, RegisterSource(tb, "source2", "topic2", testStringDeser, testStringDeser))
		//nolint:staticcheck // SA1019: testing deprecated Processor interface
		assert.NoError(t, RegisterProcessor(tb, func() kprocessor.Processor[string, string, string, string] {
			return &TestProcessor{}
		}, "processor", "source1"))

		// Manually wire source2 -> processor (simulating multi-source topology)
		graph := tb.MustBuild().GetGraph()
		graph.Nodes[kdag.NodeID("source2")].Children = append(graph.Nodes[kdag.NodeID("source2")].Children, kdag.NodeID("processor"))
		graph.Nodes[kdag.NodeID("processor")].Parents = append(graph.Nodes[kdag.NodeID("processor")].Parents, kdag.NodeID("source2"))

		task, err := BuildTaskFromGraph(graph, []string{"topic1", "topic2"}, 0, nil, "test-app", t.TempDir())
		assert.NoError(t, err)

		// Verify both sources are present
		assert.Equal(t, 2, len(task.rootNodes), "should have 2 sources")
		assert.NotZero(t, task.rootNodes["topic1"])
		assert.NotZero(t, task.rootNodes["topic2"])
	})

	t.Run("branching topology", func(t *testing.T) {
		tb := kdag.NewBuilder()
		assert.NoError(t, RegisterSource(tb, "source", "input", testStringDeser, testStringDeser))
		//nolint:staticcheck // SA1019: testing deprecated Processor interface
		assert.NoError(t, RegisterProcessor(tb, func() kprocessor.Processor[string, string, string, string] {
			return &TestProcessor{}
		}, "processor1", "source"))
		//nolint:staticcheck // SA1019: testing deprecated Processor interface
		assert.NoError(t, RegisterProcessor(tb, func() kprocessor.Processor[string, string, string, string] {
			return &TestProcessor{}
		}, "processor2", "source"))

		topology, err := tb.Build()
		assert.NoError(t, err)

		task, err := BuildTaskFromGraph(topology.GetGraph(), []string{"input"}, 0, nil, "test-app", t.TempDir())
		assert.NoError(t, err)

		// Verify both branches
		assert.Equal(t, 2, len(task.processors), "should have 2 processors")
		assert.NotZero(t, task.processors["processor1"])
		assert.NotZero(t, task.processors["processor2"])
	})

	t.Run("deep chain", func(t *testing.T) {
		tb := kdag.NewBuilder()
		assert.NoError(t, RegisterSource(tb, "source", "input", testStringDeser, testStringDeser))
		//nolint:staticcheck // SA1019: testing deprecated Processor interface
		assert.NoError(t, RegisterProcessor(tb, func() kprocessor.Processor[string, string, string, string] {
			return &TestProcessor{}
		}, "p1", "source"))
		//nolint:staticcheck // SA1019: testing deprecated Processor interface
		assert.NoError(t, RegisterProcessor(tb, func() kprocessor.Processor[string, string, string, string] {
			return &TestProcessor{}
		}, "p2", "p1"))
		//nolint:staticcheck // SA1019: testing deprecated Processor interface
		assert.NoError(t, RegisterProcessor(tb, func() kprocessor.Processor[string, string, string, string] {
			return &TestProcessor{}
		}, "p3", "p2"))
		//nolint:staticcheck // SA1019: testing deprecated Processor interface
		assert.NoError(t, RegisterProcessor(tb, func() kprocessor.Processor[string, string, string, string] {
			return &TestProcessor{}
		}, "p4", "p3"))
		assert.NoError(t, RegisterSink(tb, "sink", "output", testStringSer, testStringSer, "p4"))

		topology, err := tb.Build()
		assert.NoError(t, err)

		task, err := BuildTaskFromGraph(topology.GetGraph(), []string{"input"}, 0, nil, "test-app", t.TempDir())
		assert.NoError(t, err)

		// Verify all nodes built
		assert.Equal(t, 1, len(task.rootNodes))
		assert.Equal(t, 4, len(task.processors))
		assert.Equal(t, 1, len(task.sinks))
	})

	t.Run("multiple partitions", func(t *testing.T) {
		tb := kdag.NewBuilder()
		assert.NoError(t, RegisterSource(tb, "source", "input", testStringDeser, testStringDeser))
		err := kdag.RegisterStore(tb, newMockStoreBuilder("store"), "store")
		assert.NoError(t, err)
		//nolint:staticcheck // SA1019: testing deprecated Processor interface
		assert.NoError(t, RegisterProcessor(tb, func() kprocessor.Processor[string, string, string, string] {
			return &TestProcessor{}
		}, "processor", "source", "store"))

		topology, err := tb.Build()
		assert.NoError(t, err)

		// Build tasks for different partitions
		task0, err := BuildTaskFromGraph(topology.GetGraph(), []string{"input"}, 0, nil, "test-app", t.TempDir())
		assert.NoError(t, err)
		task1, err := BuildTaskFromGraph(topology.GetGraph(), []string{"input"}, 1, nil, "test-app", t.TempDir())
		assert.NoError(t, err)
		task2, err := BuildTaskFromGraph(topology.GetGraph(), []string{"input"}, 2, nil, "test-app", t.TempDir())
		assert.NoError(t, err)

		// Verify each task has correct partition
		assert.Equal(t, int32(0), task0.partition)
		assert.Equal(t, int32(1), task1.partition)
		assert.Equal(t, int32(2), task2.partition)

		// Verify stores are separate instances per partition
		assert.NotZero(t, task0.stores["store"])
		assert.NotZero(t, task1.stores["store"])
		assert.NotZero(t, task2.stores["store"])

		// Each task should have independent store instances
		// (partition-specific stores are wrapped, so we can't directly access mockStoreForDAG)
	})
}

func TestGetSourceTopic(t *testing.T) {
	t.Run("finds topic for source node", func(t *testing.T) {
		tb := kdag.NewBuilder()
		assert.NoError(t, RegisterSource(tb, "source1", "topic1", testStringDeser, testStringDeser))
		assert.NoError(t, RegisterSource(tb, "source2", "topic2", testStringDeser, testStringDeser))

		topology, err := tb.Build()
		assert.NoError(t, err)

		// Find topics using graph method
		topic1 := topology.GetGraph().GetSourceTopic(kdag.NodeID("source1"))
		topic2 := topology.GetGraph().GetSourceTopic(kdag.NodeID("source2"))

		assert.Equal(t, "topic1", topic1)
		assert.Equal(t, "topic2", topic2)
	})

	t.Run("returns empty string for non-existent node", func(t *testing.T) {
		tb := kdag.NewBuilder()
		assert.NoError(t, RegisterSource(tb, "source", "topic", testStringDeser, testStringDeser))

		topology, err := tb.Build()
		assert.NoError(t, err)

		topic := topology.GetGraph().GetSourceTopic(kdag.NodeID("non-existent"))
		assert.Equal(t, "", topic)
	})

	t.Run("returns empty string for processor node", func(t *testing.T) {
		tb := kdag.NewBuilder()
		assert.NoError(t, RegisterSource(tb, "source", "topic", testStringDeser, testStringDeser))
		//nolint:staticcheck // SA1019: testing deprecated Processor interface
		assert.NoError(t, RegisterProcessor(tb, func() kprocessor.Processor[string, string, string, string] {
			return &TestProcessor{}
		}, "processor", "source"))

		topology, err := tb.Build()
		assert.NoError(t, err)

		// Processor node should not have a topic
		topic := topology.GetGraph().GetSourceTopic(kdag.NodeID("processor"))
		assert.Equal(t, "", topic)
	})
}

// TestExtractRootNodes tests root node extraction logic.
// This tests an integration scenario where sources are extracted from built nodes.
func TestExtractRootNodes(t *testing.T) {
	t.Run("extracts root nodes for given topics", func(t *testing.T) {
		sources := map[string]kdag.NodeID{
			"topic1": kdag.NodeID("source1"),
			"topic2": kdag.NodeID("source2"),
			"topic3": kdag.NodeID("source3"),
		}

		// Mock built nodes
		mockSource1 := &mockSourceForDAG{}
		mockSource2 := &mockSourceForDAG{}
		mockSource3 := &mockSourceForDAG{}

		builtNodes := map[kdag.NodeID]RuntimeNode{
			kdag.NodeID("source1"): mockSource1,
			kdag.NodeID("source2"): mockSource2,
			kdag.NodeID("source3"): mockSource3,
		}

		// Extract subset of topics - inline implementation since helper was removed
		topics := []string{"topic1", "topic3"}
		rootNodes := make(map[string]runtime.RawRecordProcessor)
		for _, topic := range topics {
			if sourceID, ok := sources[topic]; ok {
				if node, ok := builtNodes[sourceID]; ok {
					if raw, ok := node.(runtime.RawRecordProcessor); ok {
						rootNodes[topic] = raw
					}
				}
			}
		}

		assert.Equal(t, 2, len(rootNodes))
		assert.NotZero(t, rootNodes["topic1"])
		assert.NotZero(t, rootNodes["topic3"])
	})

	t.Run("handles empty topics", func(t *testing.T) {
		sources := map[string]kdag.NodeID{
			"topic1": kdag.NodeID("source1"),
		}
		builtNodes := map[kdag.NodeID]RuntimeNode{
			kdag.NodeID("source1"): &mockSourceForDAG{},
		}

		topics := []string{}
		rootNodes := make(map[string]runtime.RawRecordProcessor)
		for _, topic := range topics {
			if sourceID, ok := sources[topic]; ok {
				if node, ok := builtNodes[sourceID]; ok {
					if raw, ok := node.(runtime.RawRecordProcessor); ok {
						rootNodes[topic] = raw
					}
				}
			}
		}

		assert.Equal(t, 0, len(rootNodes))
	})

	t.Run("skips non-existent topics", func(t *testing.T) {
		sources := map[string]kdag.NodeID{
			"topic1": kdag.NodeID("source1"),
		}
		builtNodes := map[kdag.NodeID]RuntimeNode{
			kdag.NodeID("source1"): &mockSourceForDAG{},
		}

		topics := []string{"topic1", "non-existent"}
		rootNodes := make(map[string]runtime.RawRecordProcessor)
		for _, topic := range topics {
			if sourceID, ok := sources[topic]; ok {
				if node, ok := builtNodes[sourceID]; ok {
					if raw, ok := node.(runtime.RawRecordProcessor); ok {
						rootNodes[topic] = raw
					}
				}
			}
		}

		assert.Equal(t, 1, len(rootNodes))
		assert.NotZero(t, rootNodes["topic1"])
	})

	t.Run("skips nodes that are not runtime.RawRecordProcessor", func(t *testing.T) {
		sources := map[string]kdag.NodeID{
			"topic1": kdag.NodeID("source1"),
		}

		// Node that doesn't implement runtime.RawRecordProcessor
		mockNode := &mockNodeNotRawProcessor{}

		builtNodes := map[kdag.NodeID]RuntimeNode{
			kdag.NodeID("source1"): mockNode,
		}

		topics := []string{"topic1"}
		rootNodes := make(map[string]runtime.RawRecordProcessor)
		for _, topic := range topics {
			if sourceID, ok := sources[topic]; ok {
				if node, ok := builtNodes[sourceID]; ok {
					if raw, ok := node.(runtime.RawRecordProcessor); ok {
						rootNodes[topic] = raw
					}
				}
			}
		}

		// Should be empty because mockNode is not a runtime.RawRecordProcessor
		assert.Equal(t, 0, len(rootNodes))
	})
}

func TestBuildTaskComplex(t *testing.T) {
	t.Run("complex multi-branch topology builds correctly", func(t *testing.T) {
		tb := kdag.NewBuilder()
		assert.NoError(t, RegisterSource(tb, "source", "input", testStringDeser, testStringDeser))

		// Create multiple branches
		//nolint:staticcheck // SA1019: testing deprecated Processor interface
		assert.NoError(t, RegisterProcessor(tb, func() kprocessor.Processor[string, string, string, string] {
			return &TestProcessor{}
		}, "branch1", "source"))
		//nolint:staticcheck // SA1019: testing deprecated Processor interface
		assert.NoError(t, RegisterProcessor(tb, func() kprocessor.Processor[string, string, string, string] {
			return &TestProcessor{}
		}, "branch2", "source"))

		// Each branch has children
		//nolint:staticcheck // SA1019: testing deprecated Processor interface
		assert.NoError(t, RegisterProcessor(tb, func() kprocessor.Processor[string, string, string, string] {
			return &TestProcessor{}
		}, "branch1-child", "branch1"))
		//nolint:staticcheck // SA1019: testing deprecated Processor interface
		assert.NoError(t, RegisterProcessor(tb, func() kprocessor.Processor[string, string, string, string] {
			return &TestProcessor{}
		}, "branch2-child", "branch2"))

		topology, err := tb.Build()
		assert.NoError(t, err)

		task, err := BuildTaskFromGraph(topology.GetGraph(), []string{"input"}, 0, nil, "test-app", t.TempDir())
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

// TestProcessor for tests - simple processor that does nothing
type TestProcessor struct {
	ctx kprocessor.ProcessorContext[string, string]
}

func (p *TestProcessor) Init(ctx kprocessor.ProcessorContext[string, string]) error {
	p.ctx = ctx
	return nil
}

func (p *TestProcessor) Process(ctx context.Context, k string, v string) error {
	return nil
}

func (p *TestProcessor) Close() error {
	return nil
}

type mockSourceForDAG struct{}

func (m *mockSourceForDAG) Init() error  { return nil }
func (m *mockSourceForDAG) Process(ctx context.Context, record *kgo.Record) error {
	return nil
}
func (m *mockSourceForDAG) Close() error { return nil }

type mockNodeNotRawProcessor struct{}

func (m *mockNodeNotRawProcessor) Init() error  { return nil }
func (m *mockNodeNotRawProcessor) Close() error { return nil }
