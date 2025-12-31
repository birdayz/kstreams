package kstreams

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Ensure SinkNode implements BatchInputProcessor
var _ BatchInputProcessor[string, string] = (*SinkNode[string, string])(nil)

// ProcessBatch implements batch processing for SinkNode.
// Serializes and produces multiple records to Kafka in one operation.
func (s *SinkNode[K, V]) ProcessBatch(ctx context.Context, keys []K, values []V) error {
	if len(keys) == 0 {
		return nil
	}

	if len(keys) != len(values) {
		panic("ProcessBatch: keys and values must have same length")
	}

	// Serialize all keys and values
	// TODO: In future, add BatchSerializer for optimized batch serialization
	kgoRecords := make([]*kgo.Record, len(keys))

	for i := range keys {
		key, err := s.KeySerializer(keys[i])
		if err != nil {
			return err
		}

		value, err := s.ValueSerializer(values[i])
		if err != nil {
			return err
		}

		kgoRecords[i] = &kgo.Record{
			Topic: s.topic,
			Key:   key,
			Value: value,
		}
	}

	// Produce all records in one call
	// franz-go batches these internally for optimal performance
	results := s.client.ProduceSync(ctx, kgoRecords...)

	// Collect errors (if any)
	for _, res := range results {
		if res.Err != nil {
			// Store as future for Flush() to handle
			s.futures = append(s.futures, produceResult{err: res.Err})
		}
	}

	return nil
}
