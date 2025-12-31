package kstreams

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// RecordInputProcessor is an interface for processors that can handle full Records
// This allows us to forward metadata-rich records to enhanced processors
type RecordInputProcessor[K, V any] interface {
	ProcessRecord(ctx context.Context, record Record[K, V]) error
}

// RuntimeSourceNode is the runtime instantiation of a source node
// It maintains full type safety with no `any` casts
type RuntimeSourceNode[K, V any] struct {
	id                NodeID
	topic             string
	keyDeserializer   Deserializer[K]
	valueDeserializer Deserializer[V]
	downstream        []InputProcessor[K, V] // Fully typed!
}

// Process deserializes a Kafka record and forwards it to all downstream processors
// It preserves metadata for RecordInputProcessors and falls back to bare key-value for legacy
func (n *RuntimeSourceNode[K, V]) Process(ctx context.Context, kgoRecord *kgo.Record) error {
	key, err := n.keyDeserializer(kgoRecord.Key)
	if err != nil {
		return fmt.Errorf("node %s: deserialize key: %w", n.id, err)
	}

	value, err := n.valueDeserializer(kgoRecord.Value)
	if err != nil {
		return fmt.Errorf("node %s: deserialize value: %w", n.id, err)
	}

	// Create Record with full metadata
	rec := newRecordFromKgo(key, value, kgoRecord)

	// Forward to downstream processors
	for _, proc := range n.downstream {
		// Try to forward as Record first (for enhanced processors)
		if recordProc, ok := proc.(RecordInputProcessor[K, V]); ok {
			if err := recordProc.ProcessRecord(ctx, rec); err != nil {
				return fmt.Errorf("node %s: forward record to downstream: %w", n.id, err)
			}
		} else {
			// CRITICAL: Set timestamp in context before calling legacy processor
			// This ensures changelog writes use the record's timestamp (not wall clock)
			// Matches Kafka Streams' behavior for event-time semantics
			if runtimeProc, ok := proc.(interface{ SetCurrentTimestamp(time.Time) }); ok {
				runtimeProc.SetCurrentTimestamp(kgoRecord.Timestamp)
			}

			// Fallback to legacy forwarding (bare key-value)
			if err := proc.Process(ctx, key, value); err != nil {
				return fmt.Errorf("node %s: forward to downstream: %w", n.id, err)
			}
		}
	}

	return nil
}

// Init initializes the source node (no-op for sources)
func (n *RuntimeSourceNode[K, V]) Init() error {
	return nil
}

// Close closes the source node (no-op for sources)
func (n *RuntimeSourceNode[K, V]) Close() error {
	return nil
}

// Verify that RuntimeSourceNode implements RawRecordProcessor
var _ RawRecordProcessor = (*RuntimeSourceNode[any, any])(nil)

// RuntimeProcessorNode is the runtime instantiation of a processor node
// It maintains full type safety with isolated error handling
// Supports both legacy Processor and new RecordProcessor interfaces
type RuntimeProcessorNode[Kin, Vin, Kout, Vout any] struct {
	id              NodeID
	userProcessor   Processor[Kin, Vin, Kout, Vout]
	recordProcessor RecordProcessor[Kin, Vin, Kout, Vout]
	context         *IsolatedProcessorContext[Kout, Vout]
	recordContext   *InternalRecordProcessorContext[Kout, Vout]
}

// Process invokes the user processor with full type safety and isolated error handling
// This is for legacy processors that receive bare key-value pairs
func (n *RuntimeProcessorNode[Kin, Vin, Kout, Vout]) Process(ctx context.Context, k Kin, v Vin) error {
	// Clear errors from previous invocation - THIS IS THE FIX!
	n.context.clearErrors()

	// Call user processor
	if err := n.userProcessor.Process(ctx, k, v); err != nil {
		return fmt.Errorf("node %s: user processor: %w", n.id, err)
	}

	// Get THIS node's errors only - no shared state!
	if errs := n.context.drainErrors(); len(errs) > 0 {
		return fmt.Errorf("node %s: forward errors: %w", n.id, errors.Join(errs...))
	}

	return nil
}

// ProcessRecord invokes the RecordProcessor with full metadata
// This implements RecordInputProcessor for enhanced processors
func (n *RuntimeProcessorNode[Kin, Vin, Kout, Vout]) ProcessRecord(ctx context.Context, record Record[Kin, Vin]) error {
	if n.recordProcessor != nil {
		// Update context with current record metadata
		if n.recordContext != nil {
			n.recordContext.currentRecord = record.Metadata
			// CRITICAL: Also update timestamp in embedded InternalProcessorContext
			// This ensures changelog writes use record timestamp (not wall clock)
			// Matches Kafka Streams' behavior for event-time semantics
			n.recordContext.InternalProcessorContext.currentTimestamp = record.Metadata.Timestamp
		}

		// Call enhanced processor
		if err := n.recordProcessor.ProcessRecord(ctx, record); err != nil {
			return fmt.Errorf("node %s: record processor: %w", n.id, err)
		}

		// Check for forwarding errors
		if n.recordContext != nil {
			if errs := n.recordContext.drainErrors(); len(errs) > 0 {
				return fmt.Errorf("node %s: forward errors: %w", n.id, errors.Join(errs...))
			}
		}

		return nil
	}

	// Fallback to legacy processor with bare key-value
	return n.Process(ctx, record.Key, record.Value)
}

// SetCurrentTimestamp sets the current timestamp in the processor context
// CRITICAL: This must be called before Process() for legacy processors
// to ensure changelog writes use the correct event timestamp
func (n *RuntimeProcessorNode[Kin, Vin, Kout, Vout]) SetCurrentTimestamp(timestamp time.Time) {
	if n.context != nil {
		n.context.currentTimestamp = timestamp
	}
	if n.recordContext != nil {
		n.recordContext.InternalProcessorContext.currentTimestamp = timestamp
	}
}

// Init initializes the processor by calling the user processor's Init
func (n *RuntimeProcessorNode[Kin, Vin, Kout, Vout]) Init() error {
	if n.recordProcessor != nil {
		return n.recordProcessor.Init(n.recordContext)
	}
	return n.userProcessor.Init(n.context)
}

// Close closes the processor
func (n *RuntimeProcessorNode[Kin, Vin, Kout, Vout]) Close() error {
	if n.recordProcessor != nil {
		return n.recordProcessor.Close()
	}
	return n.userProcessor.Close()
}

// Verify that RuntimeProcessorNode implements Node, InputProcessor, and RecordInputProcessor
var _ Node = (*RuntimeProcessorNode[any, any, any, any])(nil)
var _ InputProcessor[any, any] = (*RuntimeProcessorNode[any, any, any, any])(nil)
var _ RecordInputProcessor[any, any] = (*RuntimeProcessorNode[any, any, any, any])(nil)

// RuntimeSinkNode is the runtime instantiation of a sink node
// It maintains full type safety when serializing records
type RuntimeSinkNode[K, V any] struct {
	id              NodeID
	client          *kgo.Client
	topic           string
	keySerializer   Serializer[K]
	valueSerializer Serializer[V]

	futuresWg sync.WaitGroup
	futures   []produceResult
}

// Process serializes the key/value and produces to Kafka
func (n *RuntimeSinkNode[K, V]) Process(ctx context.Context, k K, v V) error {
	key, err := n.keySerializer(k)
	if err != nil {
		return fmt.Errorf("node %s: serialize key: %w", n.id, err)
	}

	value, err := n.valueSerializer(v)
	if err != nil {
		return fmt.Errorf("node %s: serialize value: %w", n.id, err)
	}

	n.futuresWg.Add(1)
	// Use Background context for async produce to avoid cancellation issues
	// when the processing context is canceled after Process() returns
	n.client.Produce(context.Background(), &kgo.Record{
		Key:   key,
		Value: value,
		Topic: n.topic,
	}, func(r *kgo.Record, err error) {
		pr := produceResult{
			record: r,
			err:    err,
		}
		n.futures = append(n.futures, pr)
		n.futuresWg.Done()
	})

	return nil
}

// Init initializes the sink node (no-op for sinks)
func (n *RuntimeSinkNode[K, V]) Init() error {
	return nil
}

// Close closes the sink node (no-op for sinks)
func (n *RuntimeSinkNode[K, V]) Close() error {
	return nil
}

// Flush waits for all pending produces and checks for errors
func (n *RuntimeSinkNode[K, V]) Flush(ctx context.Context) error {
	n.futuresWg.Wait()

	for _, result := range n.futures {
		if err := result.err; err != nil {
			return fmt.Errorf("node %s: produce failed: %w", n.id, result.err)
		}
	}

	// Keep allocated memory, just reset slice
	n.futures = n.futures[:0]

	return nil
}

// Verify that RuntimeSinkNode implements Node, InputProcessor, and Flusher
var _ Node = (*RuntimeSinkNode[any, any])(nil)
var _ InputProcessor[any, any] = (*RuntimeSinkNode[any, any])(nil)
var _ Flusher = (*RuntimeSinkNode[any, any])(nil)
