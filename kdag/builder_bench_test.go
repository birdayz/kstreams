package kdag

import (
	"fmt"
	"testing"

	"github.com/alecthomas/assert/v2"
)

// BenchmarkBuildSmallDAG benchmarks building a small DAG (10 nodes)
func BenchmarkBuildSmallDAG(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		builder := NewBuilder()
		assert.NoError(b, registerTestSource(builder, "source", "input"))

		parent := "source"
		for j := 0; j < 8; j++ {
			name := fmt.Sprintf("proc-%d", j)
			assert.NoError(b, registerTestProcessor(builder, name, parent))
			parent = name
		}

		assert.NoError(b, registerTestSink(builder, "sink", "output", parent))

		_, err := builder.Build()
		assert.NoError(b, err)
	}
}

// BenchmarkBuildMediumDAG benchmarks building a medium DAG (100 nodes)
func BenchmarkBuildMediumDAG(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		builder := NewBuilder()
		assert.NoError(b, registerTestSource(builder, "source", "input"))

		parent := "source"
		for j := 0; j < 98; j++ {
			name := fmt.Sprintf("proc-%d", j)
			assert.NoError(b, registerTestProcessor(builder, name, parent))
			parent = name
		}

		assert.NoError(b, registerTestSink(builder, "sink", "output", parent))

		_, err := builder.Build()
		assert.NoError(b, err)
	}
}

// BenchmarkBuildLargeDAG benchmarks building a large DAG (500 nodes)
func BenchmarkBuildLargeDAG(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		builder := NewBuilder()
		assert.NoError(b, registerTestSource(builder, "source", "input"))

		parent := "source"
		for j := 0; j < 498; j++ {
			name := fmt.Sprintf("proc-%d", j)
			assert.NoError(b, registerTestProcessor(builder, name, parent))
			parent = name
		}

		assert.NoError(b, registerTestSink(builder, "sink", "output", parent))

		_, err := builder.Build()
		assert.NoError(b, err)
	}
}

// BenchmarkBuildBranchingDAG benchmarks building a branching DAG
func BenchmarkBuildBranchingDAG(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		builder := NewBuilder()
		assert.NoError(b, registerTestSource(builder, "source", "input"))

		// Create 10 branches, each with 10 processors
		for j := 0; j < 10; j++ {
			parent := "source"
			for k := 0; k < 9; k++ {
				name := fmt.Sprintf("proc-%d-%d", j, k)
				assert.NoError(b, registerTestProcessor(builder, name, parent))
				parent = name
			}
			sinkName := fmt.Sprintf("sink-%d", j)
			assert.NoError(b, registerTestSink(builder, sinkName, "output", parent))
		}

		_, err := builder.Build()
		assert.NoError(b, err)
	}
}

// BenchmarkValidateSmallDAG benchmarks validation of a small DAG
func BenchmarkValidateSmallDAG(b *testing.B) {
	builder := NewBuilder()
	assert.NoError(b, registerTestSource(builder, "source", "input"))

	parent := "source"
	for j := 0; j < 8; j++ {
		name := fmt.Sprintf("proc-%d", j)
		assert.NoError(b, registerTestProcessor(builder, name, parent))
		parent = name
	}
	assert.NoError(b, registerTestSink(builder, "sink", "output", parent))

	graph := builder.GetGraph()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		assert.NoError(b, graph.Validate())
	}
}

// BenchmarkValidateLargeDAG benchmarks validation of a large DAG
func BenchmarkValidateLargeDAG(b *testing.B) {
	builder := NewBuilder()
	assert.NoError(b, registerTestSource(builder, "source", "input"))

	parent := "source"
	for j := 0; j < 498; j++ {
		name := fmt.Sprintf("proc-%d", j)
		assert.NoError(b, registerTestProcessor(builder, name, parent))
		parent = name
	}
	assert.NoError(b, registerTestSink(builder, "sink", "output", parent))

	graph := builder.GetGraph()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		assert.NoError(b, graph.Validate())
	}
}

// BenchmarkTopologicalSort benchmarks topological sorting
func BenchmarkTopologicalSort(b *testing.B) {
	builder := NewBuilder()
	assert.NoError(b, registerTestSource(builder, "source", "input"))

	parent := "source"
	for j := 0; j < 98; j++ {
		name := fmt.Sprintf("proc-%d", j)
		assert.NoError(b, registerTestProcessor(builder, name, parent))
		parent = name
	}
	assert.NoError(b, registerTestSink(builder, "sink", "output", parent))

	graph := builder.GetGraph()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := graph.topologicalSort()
		assert.NoError(b, err)
	}
}

// BenchmarkComputePartitionGroups benchmarks partition group computation
func BenchmarkComputePartitionGroups(b *testing.B) {
	builder := NewBuilder()

	// Create 10 independent sources, each with a chain of processors
	for i := 0; i < 10; i++ {
		sourceName := fmt.Sprintf("source-%d", i)
		topicName := fmt.Sprintf("topic-%d", i)
		assert.NoError(b, registerTestSource(builder, sourceName, topicName))

		parent := sourceName
		for j := 0; j < 10; j++ {
			procName := fmt.Sprintf("proc-%d-%d", i, j)
			assert.NoError(b, registerTestProcessor(builder, procName, parent))
			parent = procName
		}

		sinkName := fmt.Sprintf("sink-%d", i)
		assert.NoError(b, registerTestSink(builder, sinkName, "output", parent))
	}

	graph := builder.GetGraph()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := graph.ComputePartitionGroups()
		assert.NoError(b, err)
	}
}

// BenchmarkRegisterProcessor benchmarks individual processor registration
func BenchmarkRegisterProcessor(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		builder := NewBuilder()
		assert.NoError(b, registerTestSource(builder, "source", "input"))
		b.StartTimer()

		for j := 0; j < 100; j++ {
			name := fmt.Sprintf("proc-%d", j)
			parent := "source"
			if j > 0 {
				parent = fmt.Sprintf("proc-%d", j-1)
			}
			assert.NoError(b, registerTestProcessor(builder, name, parent))
		}
	}
}

// BenchmarkNodeIDValidation benchmarks NodeID validation
func BenchmarkNodeIDValidation(b *testing.B) {
	validID := NodeID("valid-node-id")
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = validID.Validate()
	}
}
