package kstreams

import (
	"context"
)

// BatchProcessor processes multiple records at once for better throughput.
// If a processor implements this interface, the framework will call ProcessBatch()
// instead of calling Process() for each record individually.
//
// ORDERING GUARANTEE:
// - Records in the slice are ALWAYS from the same partition
// - Records are ALWAYS in offset order (records[i].Offset < records[i+1].Offset)
// - This preserves the same ordering semantics as single-record processing
//
// The processor MUST NOT reorder records within the batch.
// If you need to process records out-of-order, use the single-record
// Processor interface instead.
//
// Performance benefits:
// - Reduced per-record overhead (function calls, context switches)
// - Bulk state store operations (batch writes/reads)
// - Better CPU cache locality
// - Amortized fixed costs (serialization setup, network overhead)
type BatchProcessor[Kin, Vin, Kout, Vout any] interface {
	Processor[Kin, Vin, Kout, Vout] // Embed for fallback

	// ProcessBatch processes multiple records at once.
	// All records are from the same partition and in offset order.
	//
	// The implementation can:
	// - Access state stores in bulk: store.SetBatch(kvs), store.GetBatch(keys)
	// - Forward results in batch: ctx.ForwardBatch(records)
	// - Return error to stop processing (entire batch fails)
	//
	// records[0] was received before records[1], etc.
	ProcessBatch(ctx context.Context, records []Record[Kin, Vin]) error
}

// RecordBatchProcessor is the batch version of RecordProcessor with full metadata.
type RecordBatchProcessor[Kin, Vin, Kout, Vout any] interface {
	RecordProcessor[Kin, Vin, Kout, Vout] // Embed for fallback

	// ProcessRecordBatch processes multiple records with metadata at once.
	// All records are from the same partition and in offset order.
	ProcessRecordBatch(ctx context.Context, records []Record[Kin, Vin]) error
}

// KV is a simple key-value pair for batch operations.
type KV[K, V any] struct {
	Key   K
	Value V
}

// BatchProcessorContext extends ProcessorContext with batch operations.
type BatchProcessorContext[Kout, Vout any] interface {
	ProcessorContext[Kout, Vout] // Embed existing

	// ForwardBatch forwards multiple records to all downstream processors.
	// More efficient than calling Forward() in a loop.
	ForwardBatch(ctx context.Context, records []KV[Kout, Vout]) error

	// ForwardBatchTo forwards multiple records to a specific downstream processor.
	ForwardBatchTo(ctx context.Context, records []KV[Kout, Vout], childName string) error
}

// BatchRecordProcessorContext extends RecordProcessorContext with batch operations.
type BatchRecordProcessorContext[Kout, Vout any] interface {
	RecordProcessorContext[Kout, Vout] // Embed existing

	// ForwardRecordBatch forwards multiple records with metadata to all downstream processors.
	ForwardRecordBatch(ctx context.Context, records []Record[Kout, Vout]) error

	// ForwardRecordBatchTo forwards multiple records to a specific downstream processor.
	ForwardRecordBatchTo(ctx context.Context, records []Record[Kout, Vout], childName string) error
}
