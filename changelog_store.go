package kstreams

import (
	"context"
	"fmt"
)

// ChangeloggingKeyValueStore wraps a KeyValueStore to log all changes to a changelog topic
// Matches Kafka Streams' ChangeLoggingKeyValueBytesStore
//
// Pattern: Decorator
//   - Delegates reads to inner store (no logging)
//   - Intercepts writes to log changes (dual-write)
//
// Dual-write order (critical):
//  1. Write to inner store FIRST
//  2. Log to changelog topic SECOND
//
// This ensures that if changelog write fails, we can retry without duplicating store writes.
//
// Tombstone handling:
//   - Delete(key) logs with nil value (tombstone in Kafka)
//   - Tombstones trigger log compaction to remove deleted keys
type ChangeloggingKeyValueStore[K comparable, V any] struct {
	inner        KeyValueStore[K, V]
	context      ProcessorContextInternal // Internal context with LogChange()
	name         string
	keyEncoder   func(K) ([]byte, error)
	valueEncoder func(V) ([]byte, error)
}

// NewChangeloggingKeyValueStore creates a changelog-enabled wrapper around a store
//
// Parameters:
//   - inner: The underlying store (e.g., PebbleStore)
//   - keyEncoder: Serializer for keys (e.g., StringSerializer)
//   - valueEncoder: Serializer for values (e.g., JSONSerializer)
//
// The encoders must match the decoders used in the RestoreCallback!
func NewChangeloggingKeyValueStore[K comparable, V any](
	inner KeyValueStore[K, V],
	keyEncoder func(K) ([]byte, error),
	valueEncoder func(V) ([]byte, error),
) *ChangeloggingKeyValueStore[K, V] {
	return &ChangeloggingKeyValueStore[K, V]{
		inner:        inner,
		name:         inner.Name(),
		keyEncoder:   keyEncoder,
		valueEncoder: valueEncoder,
	}
}

// Name returns the store name (delegates to inner)
func (c *ChangeloggingKeyValueStore[K, V]) Name() string {
	return c.name
}

// Init initializes the store and captures the processor context
// Context is needed for logging changes via context.LogChange()
func (c *ChangeloggingKeyValueStore[K, V]) Init(ctx ProcessorContextInternal) error {
	c.context = ctx
	// Inner store init is handled separately (during restoration)
	return nil
}

// Set writes to store, then logs to changelog
// Matches: ChangeLoggingKeyValueBytesStore.put()
func (c *ChangeloggingKeyValueStore[K, V]) Set(ctx context.Context, key K, value V) error {
	// 1. Write to inner store FIRST
	if err := c.inner.Set(ctx, key, value); err != nil {
		return fmt.Errorf("set in inner store: %w", err)
	}

	// 2. Log change to changelog topic
	if err := c.log(key, &value); err != nil {
		return fmt.Errorf("log change: %w", err)
	}

	return nil
}

// Delete removes from store, then logs tombstone to changelog
// Matches: ChangeLoggingKeyValueBytesStore.delete()
func (c *ChangeloggingKeyValueStore[K, V]) Delete(ctx context.Context, key K) error {
	// 1. Delete from inner store FIRST
	if err := c.inner.Delete(ctx, key); err != nil {
		return fmt.Errorf("delete from inner store: %w", err)
	}

	// 2. Log tombstone (nil value) to changelog
	if err := c.log(key, nil); err != nil {
		return fmt.Errorf("log tombstone: %w", err)
	}

	return nil
}

// Get reads from inner store (no changelog logging)
// Read operations don't modify state, so no logging needed
func (c *ChangeloggingKeyValueStore[K, V]) Get(ctx context.Context, key K) (V, bool, error) {
	return c.inner.Get(ctx, key)
}

// Range reads from inner store (no logging)
func (c *ChangeloggingKeyValueStore[K, V]) Range(ctx context.Context, from, to K) (KeyValueIterator[K, V], error) {
	return c.inner.Range(ctx, from, to)
}

// All reads from inner store (no logging)
func (c *ChangeloggingKeyValueStore[K, V]) All(ctx context.Context) (KeyValueIterator[K, V], error) {
	return c.inner.All(ctx)
}

// Flush flushes inner store (changelog is flushed separately by task)
func (c *ChangeloggingKeyValueStore[K, V]) Flush(ctx context.Context) error {
	return c.inner.Flush(ctx)
}

// Close closes inner store
func (c *ChangeloggingKeyValueStore[K, V]) Close() error {
	return c.inner.Close()
}

// Persistent delegates to inner store
// Matches Kafka Streams' ChangeLoggingKeyValueBytesStore.persistent()
func (c *ChangeloggingKeyValueStore[K, V]) Persistent() bool {
	return c.inner.Persistent()
}

// log serializes and logs a change to the changelog topic
// Matches: ChangeLoggingKeyValueBytesStore.log()
//
// value = nil indicates tombstone (deletion)
func (c *ChangeloggingKeyValueStore[K, V]) log(key K, value *V) error {
	// Serialize key
	keyBytes, err := c.keyEncoder(key)
	if err != nil {
		return fmt.Errorf("encode key: %w", err)
	}

	// Serialize value (nil for tombstone)
	var valueBytes []byte
	if value != nil {
		valueBytes, err = c.valueEncoder(*value)
		if err != nil {
			return fmt.Errorf("encode value: %w", err)
		}
	}
	// If value == nil, valueBytes remains nil (tombstone)

	// Log via context
	return c.context.LogChange(c.name, keyBytes, valueBytes)
}

// Verify ChangeloggingKeyValueStore implements KeyValueStore
var _ KeyValueStore[string, string] = (*ChangeloggingKeyValueStore[string, string])(nil)

// ChangeloggingBatchKeyValueStore adds batch operation logging support
// Only used if inner store implements BatchKeyValueStore
type ChangeloggingBatchKeyValueStore[K comparable, V any] struct {
	*ChangeloggingKeyValueStore[K, V]
	innerBatch BatchKeyValueStore[K, V]
}

// NewChangeloggingBatchKeyValueStore creates a changelog-enabled batch store
func NewChangeloggingBatchKeyValueStore[K comparable, V any](
	inner BatchKeyValueStore[K, V],
	keyEncoder func(K) ([]byte, error),
	valueEncoder func(V) ([]byte, error),
) *ChangeloggingBatchKeyValueStore[K, V] {
	return &ChangeloggingBatchKeyValueStore[K, V]{
		ChangeloggingKeyValueStore: NewChangeloggingKeyValueStore[K, V](inner, keyEncoder, valueEncoder),
		innerBatch:                 inner,
	}
}

// SetBatch writes batch to store, then logs each entry to changelog
// Note: Logs each entry individually (not as a single batch to Kafka)
// This matches Kafka Streams behavior (ChangeLoggingKeyValueBytesStore.putAll)
func (c *ChangeloggingBatchKeyValueStore[K, V]) SetBatch(ctx context.Context, entries []KV[K, V]) error {
	// 1. Write batch to inner store FIRST
	if err := c.innerBatch.SetBatch(ctx, entries); err != nil {
		return fmt.Errorf("set batch in inner store: %w", err)
	}

	// 2. Log each entry individually to changelog
	// (Kafka Streams does this in a loop, not as a batch produce)
	for _, entry := range entries {
		if err := c.log(entry.Key, &entry.Value); err != nil {
			return fmt.Errorf("log entry (key=%v): %w", entry.Key, err)
		}
	}

	return nil
}

// GetBatch reads from inner store (no logging)
func (c *ChangeloggingBatchKeyValueStore[K, V]) GetBatch(ctx context.Context, keys []K) ([]KV[K, V], error) {
	return c.innerBatch.GetBatch(ctx, keys)
}

// DeleteBatch deletes batch from store, then logs tombstones to changelog
func (c *ChangeloggingBatchKeyValueStore[K, V]) DeleteBatch(ctx context.Context, keys []K) error {
	// 1. Delete batch from inner store FIRST
	if err := c.innerBatch.DeleteBatch(ctx, keys); err != nil {
		return fmt.Errorf("delete batch from inner store: %w", err)
	}

	// 2. Log tombstone for each deleted key
	for _, key := range keys {
		if err := c.log(key, nil); err != nil {
			return fmt.Errorf("log tombstone (key=%v): %w", key, err)
		}
	}

	return nil
}

// Verify ChangeloggingBatchKeyValueStore implements BatchKeyValueStore
var _ BatchKeyValueStore[string, string] = (*ChangeloggingBatchKeyValueStore[string, string])(nil)
