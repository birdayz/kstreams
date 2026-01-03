package runtime

import (
	"context"
	"fmt"
	"time"

	"github.com/birdayz/kstreams/kprocessor"
	"github.com/birdayz/kstreams/kserde"
	"github.com/twmb/franz-go/pkg/kgo"
)

// RecordInputProcessor is an interface for processors that can handle full Records
// This allows us to forward metadata-rich records to enhanced processors
type RecordInputProcessor[K, V any] interface {
	ProcessRecord(ctx context.Context, record kprocessor.Record[K, V]) error
}

// RuntimeSourceNode is the runtime instantiation of a source node
// It maintains full type safety with no `any` casts
type RuntimeSourceNode[K, V any] struct {
	id                string // NodeID is just a string
	topic             string
	keyDeserializer   kserde.Deserializer[K]
	valueDeserializer kserde.Deserializer[V]
	downstream        []InputProcessor[K, V] // Fully typed!
}

// NewRuntimeSourceNode creates a new runtime source node
func NewRuntimeSourceNode[K, V any](
	id string,
	topic string,
	keyDeserializer kserde.Deserializer[K],
	valueDeserializer kserde.Deserializer[V],
) *RuntimeSourceNode[K, V] {
	return &RuntimeSourceNode[K, V]{
		id:                id,
		topic:             topic,
		keyDeserializer:   keyDeserializer,
		valueDeserializer: valueDeserializer,
		downstream:        make([]InputProcessor[K, V], 0),
	}
}

// AddDownstream adds a downstream processor to this source
func (n *RuntimeSourceNode[K, V]) AddDownstream(proc InputProcessor[K, V]) {
	n.downstream = append(n.downstream, proc)
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

// Verify that RuntimeSourceNode implements RawRecordProcessor and BatchRawRecordProcessor
var _ RawRecordProcessor = (*RuntimeSourceNode[any, any])(nil)
var _ BatchRawRecordProcessor = (*RuntimeSourceNode[any, any])(nil)

// ProcessBatch deserializes multiple Kafka records and forwards them in batch to downstream processors
func (n *RuntimeSourceNode[K, V]) ProcessBatch(ctx context.Context, records []*kgo.Record) error {
	if len(records) == 0 {
		return nil
	}

	// Deserialize all keys and values
	keys := make([]K, len(records))
	values := make([]V, len(records))

	for i, record := range records {
		key, err := n.keyDeserializer(record.Key)
		if err != nil {
			return fmt.Errorf("node %s: deserialize key: %w", n.id, err)
		}

		value, err := n.valueDeserializer(record.Value)
		if err != nil {
			return fmt.Errorf("node %s: deserialize value: %w", n.id, err)
		}

		keys[i] = key
		values[i] = value
	}

	// Forward to downstream processors
	for _, proc := range n.downstream {
		// Check if downstream supports batch processing
		if batchProc, ok := proc.(BatchInputProcessor[K, V]); ok {
			if err := batchProc.ProcessBatch(ctx, keys, values); err != nil {
				return fmt.Errorf("node %s: batch forward: %w", n.id, err)
			}
		} else {
			// Fallback: forward one-by-one with timestamp
			for i := range keys {
				if runtimeProc, ok := proc.(interface{ SetCurrentTimestamp(time.Time) }); ok {
					runtimeProc.SetCurrentTimestamp(records[i].Timestamp)
				}
				if err := proc.Process(ctx, keys[i], values[i]); err != nil {
					return fmt.Errorf("node %s: forward: %w", n.id, err)
				}
			}
		}
	}

	return nil
}

// newRecordFromKgo creates a kprocessor.Record from a kgo.Record with full metadata
func newRecordFromKgo[K, V any](key K, value V, kgoRecord *kgo.Record) kprocessor.Record[K, V] {
	headers := kprocessor.NewHeaders()
	for _, h := range kgoRecord.Headers {
		headers.Add(h.Key, h.Value)
	}

	return kprocessor.Record[K, V]{
		Key:   key,
		Value: value,
		Metadata: kprocessor.RecordMetadata{
			Topic:       kgoRecord.Topic,
			Partition:   kgoRecord.Partition,
			Offset:      kgoRecord.Offset,
			Timestamp:   kgoRecord.Timestamp,
			Headers:     headers,
			LeaderEpoch: kgoRecord.LeaderEpoch,
		},
	}
}
