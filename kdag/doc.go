// Package kdag provides a Directed Acyclic Graph (DAG) builder for stream processing topologies.
//
// # Overview
//
// kdag enables construction of type-safe, validated stream processing DAGs.
// The package separates build-time graph construction from runtime execution through
// a two-phase architecture:
//
// 1. **Build Phase**: Construct a type-safe DAG using generic registration functions
// 2. **Runtime Phase**: Execute the DAG with type-erased but validated nodes
//
// # Architecture
//
// The DAG architecture uses type erasure with type-safe closures to maintain both
// compile-time type safety and runtime flexibility:
//
//   - **Builder**: Provides type-safe registration APIs with generic type parameters
//   - **Graph**: Stores type-erased nodes with validation functions captured as closures
//   - **Node**: Build-time representation with type signatures and builder functions
//   - **DAG**: Validated, immutable graph ready for execution
//
// During registration, generic type information is captured in closures (ValidateDownstream,
// Build, Wire) allowing runtime type checking without carrying generic parameters through
// the entire system.
//
// # Basic Usage
//
//	import (
//	    "github.com/birdayz/kstreams/kdag"
//	    "github.com/birdayz/kstreams/kprocessor"
//	    "github.com/birdayz/kstreams/kserde"
//	)
//
//	// Create a new builder
//	builder := kdag.NewBuilder()
//
//	// Register a source that reads strings from Kafka
//	kdag.MustRegisterSource(builder,
//	    "input-source",           // node name
//	    "input-topic",            // Kafka topic
//	    kserde.StringDeserializer,
//	    kserde.StringDeserializer,
//	)
//
//	// Register a processor that transforms strings to ints
//	kdag.MustRegisterProcessor(builder,
//	    kprocessor.Map(func(k string, v string) int {
//	        return len(v) // transform string to its length
//	    }),
//	    "length-processor",       // node name
//	    "input-source",           // parent node
//	)
//
//	// Register a sink that writes to Kafka
//	kdag.MustRegisterSink(builder,
//	    "output-sink",            // node name
//	    "output-topic",           // Kafka topic
//	    kserde.StringSerializer,
//	    kserde.IntSerializer,
//	    "length-processor",       // parent node
//	)
//
//	// Build and validate the DAG
//	dag := builder.MustBuild()
//
// # Type Safety
//
// Type compatibility is verified at registration time. The following would fail
// at build time with ErrTypeMismatch:
//
//	// Source outputs string, string
//	kdag.MustRegisterSource(builder, "source", "topic",
//	    kserde.StringDeserializer, kserde.StringDeserializer)
//
//	// Processor expects int, int as input - TYPE MISMATCH!
//	kdag.MustRegisterProcessor(builder,
//	    kprocessor.Map(func(k int, v int) string { return "" }),
//	    "processor", "source") // FAILS: expects int but source outputs string
//
// The type system uses reflect.Type to capture and validate type signatures,
// ensuring that parent outputs match child inputs.
//
// # Validation
//
// DAG validation is performed during Build() and checks:
//
//   - **Cycle Detection**: DAGs cannot contain cycles (uses DFS)
//   - **Orphan Detection**: All nodes must be reachable from a source
//   - **Sink Validation**: Sinks cannot have children
//   - **Store Validation**: All referenced stores must be registered
//   - **Type Validation**: Parent outputs must match child inputs
//   - **Size Limits**: Prevents pathological graphs (MaxNodesPerDAG, MaxDepth, etc.)
//
// All validation errors use sentinel errors (ErrCycleDetected, ErrTypeMismatch, etc.)
// that can be checked with errors.Is().
//
// # State Stores
//
// Processors can access state stores for stateful operations:
//
//	// Register a store
//	kdag.MustRegisterStore(builder, storeBuilder, "my-store")
//
//	// Register a processor that uses the store
//	kdag.MustRegisterProcessor(builder,
//	    myProcessor,
//	    "stateful-processor",
//	    "input-source",
//	    "my-store") // reference the store
//
// Stores must be registered before any processors that reference them.
//
// # Error Handling
//
// All registration functions return errors that can be checked:
//
//	err := kdag.RegisterProcessor(builder, processor, "name", "parent")
//	if errors.Is(err, kdag.ErrNodeNotFound) {
//	    // Parent doesn't exist
//	} else if errors.Is(err, kdag.ErrTypeMismatch) {
//	    // Type incompatibility
//	}
//
// Use Must* variants to panic on error for cleaner code when errors are unexpected.
//
// # Thread Safety
//
// IMPORTANT: Builder is NOT safe for concurrent use. All registration methods
// must be called from a single goroutine. The resulting DAG is immutable and
// safe to use concurrently.
//
// # Performance
//
// The package is optimized for both build-time and runtime performance:
//
//   - Validation uses optimized algorithms (Kahn's algorithm for topological sort)
//   - Maps and slices are pre-allocated where sizes are known
//   - Deterministic ordering is maintained without excessive sorting
//   - Type checking happens at build time, not during message processing
//
// Validation complexity: O(V + E) where V is vertices and E is edges.
//
// # Advanced Usage
//
// For read-only access to the underlying graph:
//
//	graph := builder.GetGraph()
//	node, exists := builder.GetNode("my-node")
//
// WARNING: Do not modify the graph directly. Use registration functions to
// ensure proper validation.
package kdag
