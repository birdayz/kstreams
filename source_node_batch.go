package kstreams

import (
	"context"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// BatchRawRecordProcessor processes batches of raw Kafka records.
// This is the batch interface for SourceNode.
type BatchRawRecordProcessor interface {
	RawRecordProcessor
	ProcessBatch(ctx context.Context, records []*kgo.Record) error
}

// BatchInputProcessor is the batch version of InputProcessor.
type BatchInputProcessor[K, V any] interface {
	InputProcessor[K, V]
	ProcessBatch(ctx context.Context, keys []K, values []V) error
}

// Ensure SourceNode can implement BatchRawRecordProcessor
var _ BatchRawRecordProcessor = (*SourceNode[string, string])(nil)

// ProcessBatch implements batch processing for SourceNode.
// Deserializes multiple records at once and forwards them in batch to downstream processors.
func (s *SourceNode[K, V]) ProcessBatch(ctx context.Context, records []*kgo.Record) error {
	if len(records) == 0 {
		return nil
	}

	// Deserialize all keys and values
	// TODO: In future, add BatchDeserializer for optimized batch deserialization
	keys := make([]K, len(records))
	values := make([]V, len(records))

	for i, record := range records {
		key, err := s.KeyDeserializer(record.Key)
		if err != nil {
			return err
		}

		value, err := s.ValueDeserializer(record.Value)
		if err != nil {
			return err
		}

		keys[i] = key
		values[i] = value
	}

	// Forward to downstream processors
	for _, processor := range s.DownstreamProcessors {
		// Check if downstream supports batch processing
		if batchProc, ok := processor.(BatchInputProcessor[K, V]); ok {
			// NOTE: BatchInputProcessor interface doesn't carry timestamps
			// This means changelog writes in batch processors will use stale/zero timestamps
			// TODO: Consider extending interface to ProcessBatch(ctx, []Record[K,V]) to preserve timestamps
			if err := batchProc.ProcessBatch(ctx, keys, values); err != nil {
				return err
			}
		} else {
			// Fallback: forward one-by-one
			// CRITICAL: Set timestamp for each record before forwarding
			// This ensures changelog writes use correct event timestamp
			for i := range keys {
				// Set timestamp in processor context if it supports it
				if runtimeProc, ok := processor.(interface{ SetCurrentTimestamp(time.Time) }); ok {
					runtimeProc.SetCurrentTimestamp(records[i].Timestamp)
				}

				if err := processor.Process(ctx, keys[i], values[i]); err != nil {
					return err
				}
			}
		}
	}

	return nil
}
