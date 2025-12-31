package pebble

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/birdayz/kstreams"
	"github.com/cockroachdb/pebble"
)

// KeyValueStoreBuilder builds a Pebble-backed KeyValueStore with optional changelog
// Matches Kafka Streams' KeyValueStoreBuilder
//
// Example usage:
//
//	builder := pebble.NewKeyValueStoreBuilder[string, User](
//	    "user-store",
//	    "/tmp/state",
//	).WithChangelogEnabled(map[string]string{
//	    "retention.ms": "86400000",  // 1 day
//	    "cleanup.policy": "compact",
//	})
//	store := builder.Build()
type KeyValueStoreBuilder[K comparable, V any] struct {
	*kstreams.BaseStoreBuilder[kstreams.KeyValueStore[K, V]]
	name       string
	stateDir   string
	keyEncoder func(K) ([]byte, error)
	keyDecoder func([]byte) (K, error)
	valEncoder func(V) ([]byte, error)
	valDecoder func([]byte) (V, error)
}

// NewKeyValueStoreBuilder creates a new builder for a Pebble KeyValueStore
//
// Parameters:
//   - name: Store name (used for changelog topic naming)
//   - stateDir: Directory for Pebble database files
//
// Type parameters:
//   - K: Key type (must be comparable)
//   - V: Value type
//
// Default configuration:
//   - Changelog: ENABLED (matches Kafka Streams default for persistent stores)
//   - Serdes: Must be provided via WithSerdes()
func NewKeyValueStoreBuilder[K comparable, V any](
	name string,
	stateDir string,
) *KeyValueStoreBuilder[K, V] {
	return &KeyValueStoreBuilder[K, V]{
		BaseStoreBuilder: kstreams.NewBaseStoreBuilder[kstreams.KeyValueStore[K, V]](name),
		name:             name,
		stateDir:         stateDir,
	}
}

// WithSerdes configures serializers and deserializers
// REQUIRED before calling Build()
//
// Example:
//
//	builder.WithSerdes(
//	    serde.StringSerializer(), serde.StringDeserializer(),
//	    serde.JSONSerializer[User](), serde.JSONDeserializer[User](),
//	)
func (b *KeyValueStoreBuilder[K, V]) WithSerdes(
	keyEncoder func(K) ([]byte, error),
	keyDecoder func([]byte) (K, error),
	valEncoder func(V) ([]byte, error),
	valDecoder func([]byte) (V, error),
) *KeyValueStoreBuilder[K, V] {
	b.keyEncoder = keyEncoder
	b.keyDecoder = keyDecoder
	b.valEncoder = valEncoder
	b.valDecoder = valDecoder
	return b
}

// WithChangelogEnabled enables changelog with optional topic configuration
func (b *KeyValueStoreBuilder[K, V]) WithChangelogEnabled(config map[string]string) kstreams.StoreBuilder[kstreams.KeyValueStore[K, V]] {
	b.BaseStoreBuilder.WithChangelogEnabled(config)
	return b
}

// WithChangelogDisabled disables changelog
func (b *KeyValueStoreBuilder[K, V]) WithChangelogDisabled() kstreams.StoreBuilder[kstreams.KeyValueStore[K, V]] {
	b.BaseStoreBuilder.WithChangelogDisabled()
	return b
}

// Build constructs the KeyValueStore with configured options
//
// If changelog is enabled:
//  1. Creates inner Pebble store
//  2. Wraps in ChangeloggingKeyValueStore
//  3. Sets up restore callback
//
// If changelog is disabled:
//  1. Creates inner Pebble store directly
//
// Returns error if:
//   - Serdes not configured (must call WithSerdes first)
//   - State directory can't be created
//   - Pebble database can't be opened
func (b *KeyValueStoreBuilder[K, V]) Build() kstreams.KeyValueStore[K, V] {
	if b.keyEncoder == nil || b.keyDecoder == nil || b.valEncoder == nil || b.valDecoder == nil {
		panic(fmt.Sprintf("serdes not configured for store %s (must call WithSerdes)", b.name))
	}

	// Create inner Pebble store
	innerStore := b.buildInnerStore()

	// If changelog disabled, return inner store directly
	if !b.ChangelogEnabled() {
		return innerStore
	}

	// Wrap with changelog logging
	changelogStore := kstreams.NewChangeloggingKeyValueStore[K, V](
		innerStore,
		b.keyEncoder,
		b.valEncoder,
	)

	// Create restore callback
	restoreCallback := kstreams.NewBatchingKeyValueStoreRestoreCallback[K, V](
		innerStore, // Restore directly to inner store (bypass changelog wrapper)
		b.keyDecoder,
		b.valDecoder,
	)
	b.BaseStoreBuilder.SetRestoreCallback(restoreCallback)

	return changelogStore
}

// buildInnerStore creates the underlying Pebble store
func (b *KeyValueStoreBuilder[K, V]) buildInnerStore() kstreams.KeyValueStore[K, V] {
	// Pebble database path
	dbPath := filepath.Join(b.stateDir, b.name)

	// Open Pebble database
	db, err := pebble.Open(dbPath, &pebble.Options{
		// Disable write-ahead log sync for better performance
		// (changelog provides durability)
		DisableWAL: b.ChangelogEnabled(), // Only disable if changelog enabled
	})
	if err != nil {
		panic(fmt.Sprintf("failed to open pebble database for store %s: %v", b.name, err))
	}

	// Create type-safe wrapper
	return newKeyValueStore[K, V](
		b.name,
		db,
		b.keyEncoder,
		b.keyDecoder,
		b.valEncoder,
		b.valDecoder,
	)
}

// keyValueStore is a type-safe wrapper around Pebble
// Implements kstreams.KeyValueStore[K, V]
type keyValueStore[K comparable, V any] struct {
	name       string
	db         *pebble.DB
	keyEncoder func(K) ([]byte, error)
	keyDecoder func([]byte) (K, error)
	valEncoder func(V) ([]byte, error)
	valDecoder func([]byte) (V, error)
}

func newKeyValueStore[K comparable, V any](
	name string,
	db *pebble.DB,
	keyEncoder func(K) ([]byte, error),
	keyDecoder func([]byte) (K, error),
	valEncoder func(V) ([]byte, error),
	valDecoder func([]byte) (V, error),
) *keyValueStore[K, V] {
	return &keyValueStore[K, V]{
		name:       name,
		db:         db,
		keyEncoder: keyEncoder,
		keyDecoder: keyDecoder,
		valEncoder: valEncoder,
		valDecoder: valDecoder,
	}
}

func (s *keyValueStore[K, V]) Name() string {
	return s.name
}

func (s *keyValueStore[K, V]) Init(ctx kstreams.ProcessorContextInternal) error {
	// Pebble is already initialized in constructor
	return nil
}

func (s *keyValueStore[K, V]) Get(ctx context.Context, key K) (V, bool, error) {
	var zero V

	keyBytes, err := s.keyEncoder(key)
	if err != nil {
		return zero, false, fmt.Errorf("encode key: %w", err)
	}

	valueBytes, closer, err := s.db.Get(keyBytes)
	if err != nil {
		if pebble.ErrNotFound == err {
			return zero, false, nil
		}
		return zero, false, fmt.Errorf("pebble get: %w", err)
	}
	defer closer.Close()

	// Copy bytes before closer is called
	valueBytesCopy := make([]byte, len(valueBytes))
	copy(valueBytesCopy, valueBytes)

	value, err := s.valDecoder(valueBytesCopy)
	if err != nil {
		return zero, false, fmt.Errorf("decode value: %w", err)
	}

	return value, true, nil
}

func (s *keyValueStore[K, V]) Set(ctx context.Context, key K, value V) error {
	keyBytes, err := s.keyEncoder(key)
	if err != nil {
		return fmt.Errorf("encode key: %w", err)
	}

	valueBytes, err := s.valEncoder(value)
	if err != nil {
		return fmt.Errorf("encode value: %w", err)
	}

	if err := s.db.Set(keyBytes, valueBytes, &pebble.WriteOptions{Sync: false}); err != nil {
		return fmt.Errorf("pebble set: %w", err)
	}

	return nil
}

func (s *keyValueStore[K, V]) Delete(ctx context.Context, key K) error {
	keyBytes, err := s.keyEncoder(key)
	if err != nil {
		return fmt.Errorf("encode key: %w", err)
	}

	if err := s.db.Delete(keyBytes, &pebble.WriteOptions{Sync: false}); err != nil {
		return fmt.Errorf("pebble delete: %w", err)
	}

	return nil
}

func (s *keyValueStore[K, V]) Range(ctx context.Context, from, to K) (kstreams.Iterator[K, V], error) {
	// TODO: Implement range iteration
	return nil, fmt.Errorf("Range not yet implemented")
}

func (s *keyValueStore[K, V]) All(ctx context.Context) (kstreams.Iterator[K, V], error) {
	// TODO: Implement full iteration
	return nil, fmt.Errorf("All not yet implemented")
}

func (s *keyValueStore[K, V]) Flush(ctx context.Context) error {
	return s.db.Flush()
}

func (s *keyValueStore[K, V]) Close() error {
	if err := s.db.Flush(); err != nil {
		return err
	}
	return s.db.Close()
}

// Persistent returns true since Pebble stores persist data to disk
// Matches Kafka Streams' RocksDBStore.persistent() = true
func (s *keyValueStore[K, V]) Persistent() bool {
	return true
}

// Verify keyValueStore implements KeyValueStore
var _ kstreams.KeyValueStore[string, string] = (*keyValueStore[string, string])(nil)
