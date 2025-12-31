package kstreams

import (
	"context"
	"errors"
)

var (
	ErrKeyNotFound = errors.New("store: key not found")
)

// StateStore is the base interface for all state stores
// Matches Kafka Streams' org.apache.kafka.streams.processor.StateStore
type StateStore interface {
	// Name returns the store name
	Name() string

	// Init initializes the store with processor context
	Init(ctx ProcessorContextInternal) error

	// Flush persists any cached data
	Flush(ctx context.Context) error

	// Close closes the store
	Close() error

	// Persistent returns true if the store persists data to disk
	// Returns false for in-memory stores
	// CRITICAL: Only persistent stores should be checkpointed
	// Matches Kafka Streams' StateStore.persistent()
	Persistent() bool
}

// KeyValueStore is a key-value state store interface
// Matches Kafka Streams' org.apache.kafka.streams.state.KeyValueStore
type KeyValueStore[K comparable, V any] interface {
	StateStore

	// Get retrieves a value by key
	// Returns (value, true, nil) if found
	// Returns (zero, false, nil) if not found
	// Returns (zero, false, err) on error
	Get(ctx context.Context, key K) (V, bool, error)

	// Set stores a key-value pair
	Set(ctx context.Context, key K, value V) error

	// Delete removes a key
	Delete(ctx context.Context, key K) error

	// Range returns an iterator over a range of keys [from, to)
	Range(ctx context.Context, from, to K) (KeyValueIterator[K, V], error)

	// All returns an iterator over all keys
	All(ctx context.Context) (KeyValueIterator[K, V], error)
}

// BatchKeyValueStore extends KeyValueStore with batch operations
// kstreams-specific (not in Kafka Streams)
type BatchKeyValueStore[K comparable, V any] interface {
	KeyValueStore[K, V]

	// SetBatch stores multiple key-value pairs atomically
	SetBatch(ctx context.Context, entries []KV[K, V]) error

	// GetBatch retrieves multiple values by keys
	GetBatch(ctx context.Context, keys []K) ([]KV[K, V], error)

	// DeleteBatch removes multiple keys atomically
	DeleteBatch(ctx context.Context, keys []K) error
}

// KeyValueIterator is an iterator over key-value pairs
// Matches Kafka Streams' KeyValueIterator
type KeyValueIterator[K, V any] interface {
	// HasNext returns true if there are more elements
	HasNext() bool

	// Next returns the next key-value pair
	Next() (K, V, error)

	// Close closes the iterator
	Close() error
}

// WindowedStore is a windowed state store interface
// Matches Kafka Streams' org.apache.kafka.streams.state.WindowStore
type WindowedStore[K comparable, V any] interface {
	StateStore

	// Set stores a value for a key at a specific timestamp
	Set(ctx context.Context, key K, value V, timestamp int64) error

	// Get retrieves a value by key at a specific timestamp
	Get(ctx context.Context, key K, timestamp int64) (V, bool, error)

	// Fetch retrieves values for a key within a time range
	Fetch(ctx context.Context, key K, timeFrom, timeTo int64) (KeyValueIterator[K, V], error)

	// Delete removes a value for a key at a specific timestamp
	Delete(ctx context.Context, key K, timestamp int64) error
}

// StoreBackend is the old backend interface (DEPRECATED)
// Will be removed in future versions
type StoreBackend interface {
	Store
	Set(k, v []byte) error
	Get(k []byte) ([]byte, error)
	Delete(k []byte) error
	Range(lower, upper []byte) (Iterator, error)
	All() (Iterator, error)
}

// RegisterStore registers a store builder with the topology
func RegisterStore(t *TopologyBuilder, storeBuilder StoreBuilder[StateStore], name string) {
	t.graph.stores[name] = &StoreDefinition{
		Name:    name,
		Builder: storeBuilder,
	}
}
