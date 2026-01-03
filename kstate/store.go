package kstate

import (
	"context"
	"errors"
	"iter"

	"github.com/birdayz/kstreams/kprocessor"
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
	Init(ctx kprocessor.ProcessorContextInternal) error

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
	Range(ctx context.Context, from, to K) iter.Seq2[K, V]

	// All returns an iterator over all keys
	All(ctx context.Context) iter.Seq2[K, V]
}

// BatchKeyValueStore extends KeyValueStore with batch operations
// kstreams-specific (not in Kafka Streams)
type BatchKeyValueStore[K comparable, V any] interface {
	KeyValueStore[K, V]

	// SetBatch stores multiple key-value pairs atomically
	SetBatch(ctx context.Context, entries []kprocessor.KV[K, V]) error

	// GetBatch retrieves multiple values by keys
	GetBatch(ctx context.Context, keys []K) ([]kprocessor.KV[K, V], error)

	// DeleteBatch removes multiple keys atomically
	DeleteBatch(ctx context.Context, keys []K) error
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
	Fetch(ctx context.Context, key K, timeFrom, timeTo int64) iter.Seq2[K, V]

	// Delete removes a value for a key at a specific timestamp
	Delete(ctx context.Context, key K, timestamp int64) error
}

// StoreBackend is the low-level byte-oriented store interface.
// Implemented by pebble and s3 stores.
type StoreBackend interface {
	StateStore
	Set(k, v []byte) error
	Get(k []byte) ([]byte, error)
	Delete(k []byte) error
	Range(lower, upper []byte) iter.Seq2[[]byte, []byte]
	All() iter.Seq2[[]byte, []byte]
}

