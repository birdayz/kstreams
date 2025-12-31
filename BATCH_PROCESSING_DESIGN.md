# Batch Processing Design for kstreams

## Vision

Enable **end-to-end batch processing** across the entire topology:
- **Source** → reads batch from Kafka
- **Processor** → processes batch (or falls back to single-record)
- **State Store** → bulk writes/reads
- **Sink** → writes batch to Kafka

When all components support batching, achieve **10-100x better throughput** for I/O-intensive workloads.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                      Kafka (Input Topics)                       │
└─────────────────────────────────────────────────────────────────┘
                            │
                            │ Batch Read (franz-go already does this)
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                       Source Node                               │
│  - Receives: []kgo.Record                                       │
│  - Deserializes: BatchDeserializer OR fallback to single        │
│  - Forwards: Batch[K,V] OR []Record[K,V]                        │
└─────────────────────────────────────────────────────────────────┘
                            │
                            │ Batch or Single
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                      Processor Node                             │
│  - Implements: BatchProcessor[Kin,Vin,Kout,Vout]                │
│  - OR falls back to: Processor[Kin,Vin,Kout,Vout]               │
│  - State access: store.SetBatch(), store.GetBatch()             │
│  - Forwards: Batch[Kout,Vout] OR []Record[Kout,Vout]            │
└─────────────────────────────────────────────────────────────────┘
                            │
                            │ Batch or Single
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                         Sink Node                               │
│  - Receives: Batch[K,V] OR []Record[K,V]                        │
│  - Serializes: BatchSerializer OR fallback to single            │
│  - Produces: franz-go ProduceSync(records...)                   │
└─────────────────────────────────────────────────────────────────┘
                            │
                            │ Batch Write (franz-go already does this)
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                     Kafka (Output Topics)                       │
└─────────────────────────────────────────────────────────────────┘
```

---

## API Design

### 1. Batch Processor Interface

```go
// BatchProcessor processes multiple records at once for better throughput.
// If a processor implements this interface, the framework will call ProcessBatch()
// instead of calling Process() for each record individually.
//
// Performance benefits:
// - Reduced per-record overhead (function calls, context switches)
// - Bulk state store operations (batch writes/reads)
// - Better CPU cache locality
// - Amortized fixed costs (serialization setup, network overhead)
type BatchProcessor[Kin, Vin, Kout, Vout any] interface {
	Processor[Kin, Vin, Kout, Vout] // Embed for fallback

	// ProcessBatch processes multiple records at once.
	// The implementation can:
	// - Access state stores in bulk: ctx.GetStore().GetBatch(keys)
	// - Forward results in batch: ctx.ForwardBatch(records)
	// - Return error to stop processing (all records in batch marked as failed)
	ProcessBatch(ctx context.Context, records []Record[Kin, Vin]) error
}

// RecordBatchProcessor is the batch version of RecordProcessor with full metadata.
type RecordBatchProcessor[Kin, Vin, Kout, Vout any] interface {
	RecordProcessor[Kin, Vin, Kout, Vout] // Embed for fallback

	// ProcessBatch processes multiple records with metadata at once.
	ProcessRecordBatch(ctx context.Context, records []Record[Kin, Vin]) error
}
```

### 2. Batch-Aware ProcessorContext

```go
// BatchProcessorContext extends ProcessorContext with batch operations.
type BatchProcessorContext[Kout, Vout any] interface {
	ProcessorContext[Kout, Vout] // Embed existing

	// ForwardBatch forwards multiple records to all downstream processors.
	// More efficient than calling Forward() in a loop.
	ForwardBatch(ctx context.Context, records []KV[Kout, Vout]) error

	// ForwardBatchTo forwards multiple records to a specific downstream processor.
	ForwardBatchTo(ctx context.Context, records []KV[Kout, Vout], childName string) error

	// GetBatchStore returns a batch-capable store (if available).
	GetBatchStore(name string) BatchStore
}

// KV is a simple key-value pair for batch forwarding.
type KV[K, V any] struct {
	Key   K
	Value V
}
```

### 3. Batch State Store Interface

```go
// BatchStore extends Store with bulk operations.
type BatchStore interface {
	Store

	// Batch operations return false if not supported by backend.
	SupportsBatching() bool
}

// BatchKeyValueStore provides bulk read/write operations.
type BatchKeyValueStore[K, V any] interface {
	// SetBatch writes multiple key-value pairs in a single operation.
	// For stores like Pebble/RocksDB, this uses WriteBatch for atomicity and performance.
	SetBatch(kvs []KV[K, V]) error

	// GetBatch retrieves multiple values for given keys.
	// Missing keys are omitted from results (not an error).
	GetBatch(keys []K) ([]KV[K, V], error)

	// DeleteBatch removes multiple keys in a single operation.
	DeleteBatch(keys []K) error
}
```

### 4. Batch Serialization

```go
// BatchSerializer can serialize multiple values more efficiently than
// calling Serializer in a loop (e.g., reuse buffers, amortize setup costs).
type BatchSerializer[T any] func(values []T) ([][]byte, error)

// BatchDeserializer can deserialize multiple values more efficiently.
type BatchDeserializer[T any] func(data [][]byte) ([]T, error)

// SerDeBatch contains optional batch serialization methods.
type SerDeBatch[T any] struct {
	SerDe[T] // Embed single-record serde

	// Optional: if nil, falls back to calling Serializer in loop
	BatchSerializer   BatchSerializer[T]
	BatchDeserializer BatchDeserializer[T]
}
```

---

## Implementation Strategy

### Phase 1: Core Infrastructure

**1.1 Update Task to support batch flow**

```go
// Current: task.go:44
func (t *Task) Process(ctx context.Context, records ...*kgo.Record) error {
	// Group records by topic for batch processing
	byTopic := make(map[string][]*kgo.Record)
	for _, record := range records {
		byTopic[record.Topic] = append(byTopic[record.Topic], record)
	}

	// Process each topic's batch
	for topic, batch := range byTopic {
		sourceNode := t.rootNodes[topic]

		// Check if source supports batch processing
		if batchSource, ok := sourceNode.(BatchSourceNode); ok {
			if err := batchSource.ProcessBatch(ctx, batch); err != nil {
				return err
			}
		} else {
			// Fallback: process one-by-one
			for _, record := range batch {
				if err := sourceNode.Process(ctx, record); err != nil {
					return err
				}
			}
		}

		// Update offsets (always last record in batch)
		lastRecord := batch[len(batch)-1]
		t.committableOffsets[topic] = kgo.EpochOffset{
			Epoch: lastRecord.LeaderEpoch,
			Offset: lastRecord.Offset + 1,
		}
	}

	return nil
}
```

**1.2 Batch-aware Source Node**

```go
// source_node_batch.go
type BatchSourceNode[K, V any] struct {
	KeyDeserializer      Deserializer[K]
	ValueDeserializer    Deserializer[V]
	DownstreamProcessors []InputProcessor[K, V]

	// Optional batch deserializers
	KeyBatchDeserializer   BatchDeserializer[K]
	ValueBatchDeserializer BatchDeserializer[V]
}

func (s *BatchSourceNode[K, V]) ProcessBatch(ctx context.Context, records []*kgo.Record) error {
	// Try batch deserialization first
	if s.KeyBatchDeserializer != nil && s.ValueBatchDeserializer != nil {
		return s.processBatchOptimized(ctx, records)
	}

	// Fallback: deserialize one-by-one
	return s.processBatchFallback(ctx, records)
}

func (s *BatchSourceNode[K, V]) processBatchOptimized(ctx context.Context, records []*kgo.Record) error {
	// Extract raw bytes
	keys := make([][]byte, len(records))
	values := make([][]byte, len(records))
	for i, r := range records {
		keys[i] = r.Key
		values[i] = r.Value
	}

	// Batch deserialize
	deserializedKeys, err := s.KeyBatchDeserializer(keys)
	if err != nil {
		return err
	}
	deserializedValues, err := s.ValueBatchDeserializer(values)
	if err != nil {
		return err
	}

	// Forward to downstream as batch
	typedRecords := make([]Record[K, V], len(records))
	for i := range records {
		typedRecords[i] = Record[K, V]{
			Key:   deserializedKeys[i],
			Value: deserializedValues[i],
			// ... copy metadata
		}
	}

	// Check if downstream supports batch
	for _, downstream := range s.DownstreamProcessors {
		if batchProc, ok := downstream.(BatchInputProcessor[K, V]); ok {
			if err := batchProc.ProcessBatch(ctx, typedRecords); err != nil {
				return err
			}
		} else {
			// Fallback: forward one-by-one
			for _, rec := range typedRecords {
				if err := downstream.Process(ctx, rec.Key, rec.Value); err != nil {
					return err
				}
			}
		}
	}

	return nil
}
```

**1.3 Batch-aware Processor Node**

```go
// processor_node_batch.go
type BatchInputProcessor[K, V any] interface {
	InputProcessor[K, V] // Embed for fallback
	ProcessBatch(ctx context.Context, records []Record[K, V]) error
}

type ProcessorNodeBatch[Kin, Vin, Kout, Vout any] struct {
	userProcessor    BatchProcessor[Kin, Vin, Kout, Vout]
	processorContext *InternalBatchProcessorContext[Kout, Vout]
}

func (p *ProcessorNodeBatch[Kin, Vin, Kout, Vout]) ProcessBatch(
	ctx context.Context,
	records []Record[Kin, Vin],
) error {
	// Clear previous batch's errors
	p.processorContext.ClearErrors()

	// Call user's batch processor
	if err := p.userProcessor.ProcessBatch(ctx, records); err != nil {
		return err
	}

	// Check for forwarding errors
	if errs := p.processorContext.Errors(); len(errs) > 0 {
		return errors.Join(errs...)
	}

	return nil
}

// Fallback: if downstream doesn't support batch
func (p *ProcessorNodeBatch[Kin, Vin, Kout, Vout]) Process(
	ctx context.Context,
	k Kin,
	v Vin,
) error {
	// Convert to single record and call ProcessBatch with size 1
	return p.ProcessBatch(ctx, []Record[Kin, Vin]{{Key: k, Value: v}})
}
```

**1.4 Batch-aware Sink Node**

```go
// sink_node_batch.go
type BatchSinkNode[K, V any] struct {
	client            *kgo.Client
	topic             string
	KeySerializer     Serializer[K]
	ValueSerializer   Serializer[V]
	KeyBatchSerializer   BatchSerializer[K]
	ValueBatchSerializer BatchSerializer[V]

	futures []produceResult
}

func (s *BatchSinkNode[K, V]) ProcessBatch(ctx context.Context, records []Record[K, V]) error {
	var kgoRecords []*kgo.Record

	// Try batch serialization first
	if s.KeyBatchSerializer != nil && s.ValueBatchSerializer != nil {
		keys := make([]K, len(records))
		values := make([]V, len(records))
		for i, r := range records {
			keys[i] = r.Key
			values[i] = r.Value
		}

		serializedKeys, err := s.KeyBatchSerializer(keys)
		if err != nil {
			return err
		}
		serializedValues, err := s.ValueBatchSerializer(values)
		if err != nil {
			return err
		}

		kgoRecords = make([]*kgo.Record, len(records))
		for i := range records {
			kgoRecords[i] = &kgo.Record{
				Topic: s.topic,
				Key:   serializedKeys[i],
				Value: serializedValues[i],
			}
		}
	} else {
		// Fallback: serialize one-by-one
		kgoRecords = make([]*kgo.Record, len(records))
		for i, r := range records {
			key, err := s.KeySerializer(r.Key)
			if err != nil {
				return err
			}
			value, err := s.ValueSerializer(r.Value)
			if err != nil {
				return err
			}
			kgoRecords[i] = &kgo.Record{
				Topic: s.topic,
				Key:   key,
				Value: value,
			}
		}
	}

	// Produce all records in one call (franz-go batches internally)
	results := s.client.ProduceSync(ctx, kgoRecords...)
	for _, res := range results {
		if res.Err != nil {
			return res.Err
		}
	}

	return nil
}
```

### Phase 2: Batch State Stores

**2.1 Pebble Batch Store**

```go
// stores/pebble/store_batch.go
type pebbleBatchStore struct {
	*pebbleStore // Embed existing
}

func (s *pebbleBatchStore) SupportsBatching() bool {
	return true
}

func (s *pebbleBatchStore) SetBatch(kvs []KV[[]byte, []byte]) error {
	batch := s.db.NewBatch()
	defer batch.Close()

	for _, kv := range kvs {
		if kv.Value == nil {
			// Tombstone
			if err := batch.Delete(kv.Key, nil); err != nil {
				return err
			}
		} else {
			if err := batch.Set(kv.Key, kv.Value, nil); err != nil {
				return err
			}
		}
	}

	// Commit batch atomically
	return batch.Commit(&pebble.WriteOptions{Sync: false})
}

func (s *pebbleBatchStore) GetBatch(keys [][]byte) ([]KV[[]byte, []byte], error) {
	results := make([]KV[[]byte, []byte], 0, len(keys))

	// Pebble doesn't have native MultiGet, but we can optimize with iterators
	// For now, simple implementation:
	for _, key := range keys {
		value, err := s.Get(key)
		if err == kstreams.ErrKeyNotFound {
			continue // Skip missing keys
		}
		if err != nil {
			return nil, err
		}
		results = append(results, KV[[]byte, []byte]{Key: key, Value: value})
	}

	return results, nil
}

func (s *pebbleBatchStore) DeleteBatch(keys [][]byte) error {
	batch := s.db.NewBatch()
	defer batch.Close()

	for _, key := range keys {
		if err := batch.Delete(key, nil); err != nil {
			return err
		}
	}

	return batch.Commit(&pebble.WriteOptions{Sync: false})
}
```

**2.2 Typed Batch Store Wrapper**

```go
// store_batch.go
type BatchKeyValueStoreImpl[K, V any] struct {
	*KeyValueStore[K, V] // Embed existing
	batchBackend BatchStoreBackend
}

func (s *BatchKeyValueStoreImpl[K, V]) SetBatch(kvs []KV[K, V]) error {
	// Serialize all keys and values
	rawKVs := make([]KV[[]byte, []byte], len(kvs))
	for i, kv := range kvs {
		key, err := s.keySerializer(kv.Key)
		if err != nil {
			return err
		}
		value, err := s.valueSerializer(kv.Value)
		if err != nil {
			return err
		}
		rawKVs[i] = KV[[]byte, []byte]{Key: key, Value: value}
	}

	return s.batchBackend.SetBatch(rawKVs)
}

func (s *BatchKeyValueStoreImpl[K, V]) GetBatch(keys []K) ([]KV[K, V], error) {
	// Serialize keys
	rawKeys := make([][]byte, len(keys))
	for i, k := range keys {
		key, err := s.keySerializer(k)
		if err != nil {
			return nil, err
		}
		rawKeys[i] = key
	}

	// Batch get
	rawResults, err := s.batchBackend.GetBatch(rawKeys)
	if err != nil {
		return nil, err
	}

	// Deserialize results
	results := make([]KV[K, V], len(rawResults))
	for i, raw := range rawResults {
		key, err := s.keyDeserializer(raw.Key)
		if err != nil {
			return nil, err
		}
		value, err := s.valueDeserializer(raw.Value)
		if err != nil {
			return nil, err
		}
		results[i] = KV[K, V]{Key: key, Value: value}
	}

	return results, nil
}
```

### Phase 3: Example Usage

**Example: Batch Aggregator**

```go
// Example: Count events by user ID in batches
type BatchCountAggregator struct {
	store    BatchKeyValueStore[string, int64]
	ctx      BatchProcessorContext[string, int64]
}

func (p *BatchCountAggregator) Init(ctx ProcessorContext[string, int64]) error {
	p.ctx = ctx.(BatchProcessorContext[string, int64])
	p.store = p.ctx.GetBatchStore("counts").(BatchKeyValueStore[string, int64])
	return nil
}

func (p *BatchCountAggregator) ProcessBatch(
	ctx context.Context,
	records []Record[string, Event],
) error {
	// Group by user ID
	countsByUser := make(map[string]int64)
	for _, rec := range records {
		countsByUser[rec.Key]++
	}

	// Batch read current counts
	userIDs := make([]string, 0, len(countsByUser))
	for userID := range countsByUser {
		userIDs = append(userIDs, userID)
	}

	currentCounts, err := p.store.GetBatch(userIDs)
	if err != nil {
		return err
	}

	// Update counts
	currentMap := make(map[string]int64)
	for _, kv := range currentCounts {
		currentMap[kv.Key] = kv.Value
	}

	updates := make([]KV[string, int64], 0, len(countsByUser))
	outputs := make([]KV[string, int64], 0, len(countsByUser))

	for userID, increment := range countsByUser {
		newCount := currentMap[userID] + increment
		updates = append(updates, KV[string, int64]{Key: userID, Value: newCount})
		outputs = append(outputs, KV[string, int64]{Key: userID, Value: newCount})
	}

	// Batch write to store
	if err := p.store.SetBatch(updates); err != nil {
		return err
	}

	// Batch forward downstream
	return p.ctx.ForwardBatch(ctx, outputs)
}

// Fallback for single-record processing
func (p *BatchCountAggregator) Process(ctx context.Context, k string, v Event) error {
	return p.ProcessBatch(ctx, []Record[string, Event]{{Key: k, Value: v}})
}

func (p *BatchCountAggregator) Close() error {
	return nil
}
```

---

## Performance Optimization Strategies

### 1. Adaptive Batching

```go
// Task dynamically adjusts batch size based on:
// - Latency targets (don't batch too long)
// - Throughput (larger batches when backlog exists)
// - Memory pressure (smaller batches when memory is tight)

type AdaptiveBatcher struct {
	maxBatchSize    int           // Max records per batch (e.g., 1000)
	maxWaitTime     time.Duration // Max time to wait for batch (e.g., 100ms)
	targetLatencyP99 time.Duration // Target p99 latency (e.g., 500ms)

	currentBatchSize int
	metrics          *BatchMetrics
}

func (b *AdaptiveBatcher) AdjustBatchSize() {
	if b.metrics.P99Latency > b.targetLatencyP99 {
		// Reduce batch size to improve latency
		b.currentBatchSize = max(1, b.currentBatchSize/2)
	} else if b.metrics.P99Latency < b.targetLatencyP99/2 {
		// Increase batch size for better throughput
		b.currentBatchSize = min(b.maxBatchSize, b.currentBatchSize*2)
	}
}
```

### 2. Zero-Copy Forwarding

```go
// Avoid copying records when forwarding between nodes
// Use slices that reference the same underlying array

func (ctx *InternalBatchProcessorContext[K, V]) ForwardBatch(
	ctx context.Context,
	records []KV[K, V],
) error {
	for name, proc := range ctx.outputs {
		if batchProc, ok := proc.(BatchInputProcessor[K, V]); ok {
			// Zero-copy: pass same slice (processor must not mutate!)
			if err := batchProc.ProcessBatch(ctx, records); err != nil {
				ctx.outputErrors = append(ctx.outputErrors,
					fmt.Errorf("batch forward to %s failed: %w", name, err))
			}
		}
	}
	return nil
}
```

### 3. Parallel Batch Processing (Advanced)

```go
// For CPU-intensive processors, split batch across goroutines
type ParallelBatchProcessor[Kin, Vin, Kout, Vout any] struct {
	userProcessor BatchProcessor[Kin, Vin, Kout, Vout]
	parallelism   int
}

func (p *ParallelBatchProcessor[Kin, Vin, Kout, Vout]) ProcessBatch(
	ctx context.Context,
	records []Record[Kin, Vin],
) error {
	if len(records) < p.parallelism*10 {
		// Too small, process sequentially
		return p.userProcessor.ProcessBatch(ctx, records)
	}

	// Split into chunks
	chunkSize := (len(records) + p.parallelism - 1) / p.parallelism
	var wg sync.WaitGroup
	errChan := make(chan error, p.parallelism)

	for i := 0; i < len(records); i += chunkSize {
		end := min(i+chunkSize, len(records))
		chunk := records[i:end]

		wg.Add(1)
		go func(chunk []Record[Kin, Vin]) {
			defer wg.Done()
			if err := p.userProcessor.ProcessBatch(ctx, chunk); err != nil {
				errChan <- err
			}
		}(chunk)
	}

	wg.Wait()
	close(errChan)

	// Collect errors
	var errs []error
	for err := range errChan {
		errs = append(errs, err)
	}

	return errors.Join(errs...)
}
```

---

## Metrics & Observability

```go
type BatchMetrics struct {
	// Batch sizes
	BatchSizeMin    prometheus.Histogram
	BatchSizeMax    prometheus.Histogram
	BatchSizeAvg    prometheus.Histogram

	// Latency (per-batch and per-record)
	BatchLatency    prometheus.Histogram
	RecordLatency   prometheus.Histogram

	// Throughput
	RecordsPerSec   prometheus.Gauge
	BatchesPerSec   prometheus.Gauge

	// Batch effectiveness
	BatchUtilization prometheus.Gauge // actual/max batch size
	FallbackRate     prometheus.Gauge // % of times fell back to single
}
```

---

## Migration Strategy

### Backward Compatibility

1. **Existing processors continue to work** - No breaking changes
2. **Opt-in batching** - Implement `BatchProcessor` interface to enable
3. **Automatic detection** - Runtime checks `interface.(BatchProcessor)`
4. **Graceful degradation** - Falls back to single-record if any component doesn't support batching

### Rollout Plan

**Phase 1:** Infrastructure (2 weeks)
- Add batch interfaces
- Update Task, SourceNode, SinkNode
- Add metrics

**Phase 2:** State Stores (1 week)
- Pebble batch operations
- Typed batch wrappers
- Benchmarks

**Phase 3:** Built-in Processors (1 week)
- BatchForEachProcessor
- BatchAggregatorProcessor
- BatchFilterProcessor

**Phase 4:** Documentation & Examples (3 days)
- Migration guide
- Performance benchmarks
- Example applications

---

## Expected Performance Gains

Based on similar optimizations in other stream processing systems:

| Workload Type | Single-Record | Batch (size=100) | Speedup |
|---------------|---------------|------------------|---------|
| **CPU-bound** (simple transform) | 100k/s | 200k/s | **2x** |
| **Memory-bound** (aggregation) | 50k/s | 300k/s | **6x** |
| **I/O-bound** (state store writes) | 10k/s | 500k/s | **50x** |
| **Network-bound** (sink to Kafka) | 20k/s | 400k/s | **20x** |

Real-world mixed workload: **5-15x improvement** typical.

---

## Ordering Guarantees

### Kafka Streams Ordering Semantics

**Current (single-record processing):**
- ✅ **Within partition**: Strict offset order (total order)
- ❌ **Across partitions**: No ordering guarantees

**With batch processing - SAME guarantees:**
- ✅ **Within partition**: Batches contain records in offset order
- ❌ **Across partitions**: Batches may interleave partitions (no cross-partition order)

### Design Decision: Batch Granularity

**Option 1: Per-Partition Batching** ⭐ **RECOMMENDED**
```go
// Group by (topic, partition) - strictest ordering
byTopicPartition := make(map[topicPartition][]*kgo.Record)
for _, record := range records {
    key := topicPartition{topic: record.Topic, partition: record.Partition}
    byTopicPartition[key] = append(byTopicPartition[key], record)
}

// Process each partition's batch independently
for _, batch := range byTopicPartition {
    // batch is guaranteed in-order (sorted by offset)
    sourceNode.ProcessBatch(ctx, batch)
}
```

**Guarantees:**
- ✅ Each batch = same partition, sorted by offset
- ✅ Identical ordering semantics to single-record processing
- ✅ Safe for stateful processors (no cross-partition issues)
- ⚠️ Smaller batches (limited by per-partition throughput)

**Option 2: Per-Topic Batching**
```go
// Group by topic only - allows cross-partition interleaving
byTopic := make(map[string][]*kgo.Record)
for _, record := range records {
    byTopic[record.Topic] = append(byTopic[record.Topic], record)
}
```

**Guarantees:**
- ✅ Records from same partition are in-order within batch
- ❌ Records from different partitions may interleave
- ⚠️ **UNSAFE** for processors that assume partition affinity
- ✅ Larger batches (better throughput)

**Recommendation:** **Use Option 1 (per-partition batching)** to maintain identical semantics to current implementation.

### Implementation: Guaranteed In-Order Slices

```go
// task.go - Updated Process() with per-partition batching
func (t *Task) Process(ctx context.Context, records ...*kgo.Record) error {
	// CRITICAL: Kafka already delivers records in offset order
	// We must preserve this order within batches

	// Group by (topic, partition) to maintain ordering
	type partitionKey struct {
		topic     string
		partition int32
	}

	batches := make(map[partitionKey][]*kgo.Record)
	for _, record := range records {
		key := partitionKey{topic: record.Topic, partition: record.Partition}
		batches[key] = append(batches[key], record)
	}

	// Process each partition's batch
	for key, batch := range batches {
		// GUARANTEE: batch is in offset order (as delivered by Kafka)
		// The slice order = Kafka offset order
		sourceNode := t.rootNodes[key.topic]

		if batchSource, ok := sourceNode.(BatchSourceNode); ok {
			if err := batchSource.ProcessBatch(ctx, batch); err != nil {
				return err
			}
		} else {
			// Fallback maintains order
			for _, record := range batch {
				if err := sourceNode.Process(ctx, record); err != nil {
					return err
				}
			}
		}

		// Update offset (last record in batch)
		lastRecord := batch[len(batch)-1]
		t.committableOffsets[key.topic] = kgo.EpochOffset{
			Epoch:  lastRecord.LeaderEpoch,
			Offset: lastRecord.Offset + 1,
		}
	}

	return nil
}
```

### Contract for Batch Processors

**Documented guarantee for users:**

```go
// BatchProcessor processes multiple records at once.
//
// ORDERING GUARANTEE:
// - Records in the slice are ALWAYS from the same partition
// - Records are ALWAYS in offset order (records[i].Offset < records[i+1].Offset)
// - This preserves the same ordering semantics as single-record processing
//
// The processor MUST NOT reorder records within the batch.
// If you need to process records out-of-order, use the single-record
// Processor interface instead.
type BatchProcessor[Kin, Vin, Kout, Vout any] interface {
	Processor[Kin, Vin, Kout, Vout]

	// ProcessBatch processes records in order.
	// records[0] was received before records[1], etc.
	ProcessBatch(ctx context.Context, records []Record[Kin, Vin]) error
}
```

### Example: Order-Dependent Processing

```go
// Example: Session detection requires ordering
type SessionDetector struct {
	store BatchKeyValueStore[string, Session]
}

func (p *SessionDetector) ProcessBatch(
	ctx context.Context,
	records []Record[string, Event],
) error {
	// SAFE: We can rely on records being in timestamp order
	// because they're in offset order from same partition

	sessions := make(map[string]*Session)

	for _, rec := range records {
		// Process in order - critical for session logic
		session := sessions[rec.Key]
		if session == nil {
			// Load from store
			existing, _ := p.store.Get(rec.Key)
			session = existing
		}

		if rec.Value.Timestamp - session.LastEvent > sessionTimeout {
			// Timeout - start new session
			// This logic REQUIRES records to be in order
			session = &Session{Start: rec.Value.Timestamp}
		}

		session.LastEvent = rec.Value.Timestamp
		session.EventCount++
		sessions[rec.Key] = session
	}

	// Batch write updated sessions
	updates := make([]KV[string, Session], 0, len(sessions))
	for key, session := range sessions {
		updates = append(updates, KV[string, Session]{Key: key, Value: *session})
	}

	return p.store.SetBatch(updates)
}
```

### Edge Cases

**1. Single partition, large backlog:**
```
Batch size = min(maxBatchSize, available records from partition)
All records in-order ✅
```

**2. Multiple partitions, low throughput:**
```
Partition 0: [r0, r1]
Partition 1: [r3]
Partition 2: [r5, r6, r7]

Creates 3 separate batches:
- Batch 1: [r0, r1] from P0 ✅
- Batch 2: [r3] from P1 ✅
- Batch 3: [r5, r6, r7] from P2 ✅

Each batch maintains partition's order
```

**3. Rebalancing mid-batch:**
```
If rebalance happens:
1. Current batch completes (or fails)
2. Offsets committed
3. Partitions revoked
4. New assignment takes over

No partial batch commits ✅
```

## Open Questions

1. **Batch size limits?**
   - Start with configurable max (default: 1000 records or 1MB)
   - Make adaptive based on latency/throughput metrics
   - **Per-partition max** to ensure ordering

2. **Partial batch failures?**
   - Option A: Fail entire batch (simpler, better for atomicity)
   - Option B: Return `[]error` aligned with input (more complex)
   - **Recommendation:** Start with Option A

3. **Transaction semantics?**
   - Single batch = single transaction in state stores?
   - Would require distributed transaction support
   - **Recommendation:** Document that batches are NOT atomic across stores

4. **Cross-processor batching?**
   - If Processor A outputs batch, does Processor B receive it as batch?
   - **Recommendation:** Yes, preserve batch boundaries when possible
   - Falls back to single-record if downstream doesn't support batching

---

## References

- [KIP-167: Batching State Restore](https://cwiki.apache.org/confluence/display/KAFKA/KIP-167:+Add+interface+for+the+state+store+restoration+process)
- [Franz-go batch produce](https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#Client.ProduceSync)
- [Pebble WriteBatch](https://pkg.go.dev/github.com/cockroachdb/pebble#Batch)
- [Flink Batch Processing](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/operators/overview/#process-function)
