package kstate

import (
	"context"
	"fmt"

	"github.com/birdayz/kstreams/internal/checkpoint"
	"github.com/birdayz/kstreams/kprocessor"
	"github.com/twmb/franz-go/pkg/kgo"
)

// StateRestoreCallback defines how to restore records from a changelog topic into a state store
// Matches Kafka Streams' org.apache.kafka.streams.processor.StateRestoreCallback
//
// Implementations should handle:
//   - Normal records: Apply put(key, value) to store
//   - Tombstone records (nil value): Apply delete(key) to store
//
// Two restoration modes:
//   - Per-record: Restore() called for each record individually (legacy, slower)
//   - Batched: RestoreBatch() called with batches of records (modern, faster)
type StateRestoreCallback interface {
	// Restore a single record (called if RestoreBatch not implemented)
	// value = nil indicates tombstone (deletion)
	Restore(key, value []byte) error

	// RestoreBatch restores a batch of records for efficiency
	// Implementations should check for tombstones (record.Value == nil)
	RestoreBatch(records []*kgo.Record) error
}

// StateRestoreListener provides callbacks for monitoring restoration progress
// Matches Kafka Streams' org.apache.kafka.streams.processor.StateRestoreListener
//
// Used for:
//   - Progress monitoring (e.g., "Restored 1000/5000 records")
//   - Metrics collection
//   - Timeout detection (fail if restoration takes too long)
//
// Note: Must be stateless or provide internal synchronization (called from multiple goroutines)
type StateRestoreListener interface {
	// OnRestoreStart is called when restoration begins
	// startingOffset: First offset to restore (usually checkpoint offset + 1)
	// endingOffset: EXCLUSIVE ending offset (high water mark, one past the last offset to restore)
	//               Matches Kafka Streams semantics: restoration completes when reaching endingOffset - 1
	OnRestoreStart(topicPartition checkpoint.TopicPartition, storeName string, startingOffset, endingOffset int64)

	// OnBatchRestored is called after each batch is applied to the store
	// batchEndOffset: Last offset in the batch
	// numRestored: Number of records in this batch
	OnBatchRestored(topicPartition checkpoint.TopicPartition, storeName string, batchEndOffset int64, numRestored int64)

	// OnRestoreEnd is called when restoration completes
	// totalRestored: Total number of records restored
	OnRestoreEnd(topicPartition checkpoint.TopicPartition, storeName string, totalRestored int64)
}

// NoOpRestoreListener is a no-op implementation of StateRestoreListener
// Used when no monitoring is needed
type NoOpRestoreListener struct{}

func (n *NoOpRestoreListener) OnRestoreStart(topicPartition checkpoint.TopicPartition, storeName string, startingOffset, endingOffset int64) {
}

func (n *NoOpRestoreListener) OnBatchRestored(topicPartition checkpoint.TopicPartition, storeName string, batchEndOffset int64, numRestored int64) {
}

func (n *NoOpRestoreListener) OnRestoreEnd(topicPartition checkpoint.TopicPartition, storeName string, totalRestored int64) {
}

// KeyValueStoreRestoreCallback is the default restore callback for KeyValueStore
// Handles deserialization and applies records to the store
//
// Tombstone handling:
//   - record.Value == nil → store.Delete(key)
//   - record.Value != nil → store.Set(key, value)
type KeyValueStoreRestoreCallback[K comparable, V any] struct {
	store        KeyValueStore[K, V]
	keyDecoder   func([]byte) (K, error)
	valueDecoder func([]byte) (V, error)
}

// NewKeyValueStoreRestoreCallback creates a restore callback for a KeyValueStore
func NewKeyValueStoreRestoreCallback[K comparable, V any](
	store KeyValueStore[K, V],
	keyDecoder func([]byte) (K, error),
	valueDecoder func([]byte) (V, error),
) *KeyValueStoreRestoreCallback[K, V] {
	return &KeyValueStoreRestoreCallback[K, V]{
		store:        store,
		keyDecoder:   keyDecoder,
		valueDecoder: valueDecoder,
	}
}

// Restore restores a single record
func (r *KeyValueStoreRestoreCallback[K, V]) Restore(key, value []byte) error {
	k, err := r.keyDecoder(key)
	if err != nil {
		return fmt.Errorf("decode key: %w", err)
	}

	// Tombstone: delete from store
	if value == nil {
		return r.store.Delete(context.Background(), k)
	}

	// Normal record: set in store
	v, err := r.valueDecoder(value)
	if err != nil {
		return fmt.Errorf("decode value: %w", err)
	}

	return r.store.Set(context.Background(), k, v)
}

// RestoreBatch restores a batch of records
// More efficient than calling Restore() for each record individually
func (r *KeyValueStoreRestoreCallback[K, V]) RestoreBatch(records []*kgo.Record) error {
	for _, record := range records {
		if err := r.Restore(record.Key, record.Value); err != nil {
			return fmt.Errorf("restore record at offset %d: %w", record.Offset, err)
		}
	}
	return nil
}

// BatchingKeyValueStoreRestoreCallback uses batch operations if available
// Falls back to individual operations if store doesn't support batching
type BatchingKeyValueStoreRestoreCallback[K comparable, V any] struct {
	store        KeyValueStore[K, V]
	keyDecoder   func([]byte) (K, error)
	valueDecoder func([]byte) (V, error)
}

// NewBatchingKeyValueStoreRestoreCallback creates a batching restore callback
func NewBatchingKeyValueStoreRestoreCallback[K comparable, V any](
	store KeyValueStore[K, V],
	keyDecoder func([]byte) (K, error),
	valueDecoder func([]byte) (V, error),
) *BatchingKeyValueStoreRestoreCallback[K, V] {
	return &BatchingKeyValueStoreRestoreCallback[K, V]{
		store:        store,
		keyDecoder:   keyDecoder,
		valueDecoder: valueDecoder,
	}
}

// Restore restores a single record
func (r *BatchingKeyValueStoreRestoreCallback[K, V]) Restore(key, value []byte) error {
	k, err := r.keyDecoder(key)
	if err != nil {
		return fmt.Errorf("decode key: %w", err)
	}

	if value == nil {
		return r.store.Delete(context.Background(), k)
	}

	v, err := r.valueDecoder(value)
	if err != nil {
		return fmt.Errorf("decode value: %w", err)
	}

	return r.store.Set(context.Background(), k, v)
}

// RestoreBatch restores using batch operations if the store supports BatchKeyValueStore interface
func (r *BatchingKeyValueStoreRestoreCallback[K, V]) RestoreBatch(records []*kgo.Record) error {
	// Check if store supports batch operations
	if batchStore, ok := r.store.(BatchKeyValueStore[K, V]); ok {
		return r.restoreBatchOptimized(batchStore, records)
	}

	// Fallback to individual operations
	for _, record := range records {
		if err := r.Restore(record.Key, record.Value); err != nil {
			return fmt.Errorf("restore record at offset %d: %w", record.Offset, err)
		}
	}
	return nil
}

// restoreBatchOptimized uses batch operations for better performance
func (r *BatchingKeyValueStoreRestoreCallback[K, V]) restoreBatchOptimized(
	batchStore BatchKeyValueStore[K, V],
	records []*kgo.Record,
) error {
	var puts []kprocessor.KV[K, V]
	var deletes []K

	for _, record := range records {
		k, err := r.keyDecoder(record.Key)
		if err != nil {
			return fmt.Errorf("decode key: %w", err)
		}

		if record.Value == nil {
			// Tombstone: add to delete batch
			deletes = append(deletes, k)
		} else {
			// Normal record: add to put batch
			v, err := r.valueDecoder(record.Value)
			if err != nil {
				return fmt.Errorf("decode value: %w", err)
			}
			puts = append(puts, kprocessor.KV[K, V]{Key: k, Value: v})
		}
	}

	// Apply batches
	if len(puts) > 0 {
		if err := batchStore.SetBatch(context.Background(), puts); err != nil {
			return fmt.Errorf("set batch: %w", err)
		}
	}

	if len(deletes) > 0 {
		if err := batchStore.DeleteBatch(context.Background(), deletes); err != nil {
			return fmt.Errorf("delete batch: %w", err)
		}
	}

	return nil
}
