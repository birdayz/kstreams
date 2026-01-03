package kstate

import (
	"context"
	"fmt"
	"iter"

	"github.com/birdayz/kstreams/kprocessor"
)

// KeyValueStoreRef provides type-safe access to a KeyValueStore.
// Use this as a processor field to declare store dependencies at compile time.
//
// Example:
//
//	type MyProcessor struct {
//	    counter kstate.KeyValueStoreRef[string, int64]
//	}
//
//	func (p *MyProcessor) Init(ctx kprocessor.ProcessorContext[string, int]) error {
//	    return p.counter.Init(ctx, "counter-store")
//	}
//
//	func (p *MyProcessor) Process(ctx context.Context, key string, value int) error {
//	    count, _, _ := p.counter.Get(ctx, key)
//	    return p.counter.Set(ctx, key, count+1)
//	}
type KeyValueStoreRef[K comparable, V any] struct {
	store KeyValueStore[K, V]
	name  string
}

// Init initializes the store reference from the processor context.
// Must be called in the processor's Init method before using the store.
func (r *KeyValueStoreRef[K, V]) Init(ctx kprocessor.ProcessorContext[any, any], name string) error {
	store, err := GetKeyValueStore[K, V](ctx, name)
	if err != nil {
		return fmt.Errorf("init store ref %q: %w", name, err)
	}
	r.store = store
	r.name = name
	return nil
}

// InitFromAny initializes using any ProcessorContext type (for generic processors).
func InitKeyValueStoreRef[K comparable, V any, Kout, Vout any](
	r *KeyValueStoreRef[K, V],
	ctx kprocessor.ProcessorContext[Kout, Vout],
	name string,
) error {
	store, err := GetKeyValueStore[K, V](ctx, name)
	if err != nil {
		return fmt.Errorf("init store ref %q: %w", name, err)
	}
	r.store = store
	r.name = name
	return nil
}

// Name returns the store name.
func (r *KeyValueStoreRef[K, V]) Name() string {
	return r.name
}

// Get retrieves a value by key. Panics if not initialized.
func (r *KeyValueStoreRef[K, V]) Get(ctx context.Context, key K) (V, bool, error) {
	return r.store.Get(ctx, key)
}

// Set stores a key-value pair. Panics if not initialized.
func (r *KeyValueStoreRef[K, V]) Set(ctx context.Context, key K, value V) error {
	return r.store.Set(ctx, key, value)
}

// Delete removes a key. Panics if not initialized.
func (r *KeyValueStoreRef[K, V]) Delete(ctx context.Context, key K) error {
	return r.store.Delete(ctx, key)
}

// Range iterates over a key range. Panics if not initialized.
func (r *KeyValueStoreRef[K, V]) Range(ctx context.Context, from, to K) iter.Seq2[K, V] {
	return r.store.Range(ctx, from, to)
}

// All iterates over all entries. Panics if not initialized.
func (r *KeyValueStoreRef[K, V]) All(ctx context.Context) iter.Seq2[K, V] {
	return r.store.All(ctx)
}

// Store returns the underlying store (for advanced use cases).
func (r *KeyValueStoreRef[K, V]) Store() KeyValueStore[K, V] {
	return r.store
}

// BatchKeyValueStoreRef provides type-safe access to a BatchKeyValueStore.
// Use this for high-throughput scenarios with batch operations.
type BatchKeyValueStoreRef[K comparable, V any] struct {
	store BatchKeyValueStore[K, V]
	name  string
}

// Init initializes the batch store reference from the processor context.
func (r *BatchKeyValueStoreRef[K, V]) Init(ctx kprocessor.ProcessorContext[any, any], name string) error {
	store, err := GetBatchKeyValueStore[K, V](ctx, name)
	if err != nil {
		return fmt.Errorf("init batch store ref %q: %w", name, err)
	}
	r.store = store
	r.name = name
	return nil
}

// InitBatchKeyValueStoreRef initializes using any ProcessorContext type.
func InitBatchKeyValueStoreRef[K comparable, V any, Kout, Vout any](
	r *BatchKeyValueStoreRef[K, V],
	ctx kprocessor.ProcessorContext[Kout, Vout],
	name string,
) error {
	store, err := GetBatchKeyValueStore[K, V](ctx, name)
	if err != nil {
		return fmt.Errorf("init batch store ref %q: %w", name, err)
	}
	r.store = store
	r.name = name
	return nil
}

// Name returns the store name.
func (r *BatchKeyValueStoreRef[K, V]) Name() string {
	return r.name
}

// Get retrieves a value by key.
func (r *BatchKeyValueStoreRef[K, V]) Get(ctx context.Context, key K) (V, bool, error) {
	return r.store.Get(ctx, key)
}

// Set stores a key-value pair.
func (r *BatchKeyValueStoreRef[K, V]) Set(ctx context.Context, key K, value V) error {
	return r.store.Set(ctx, key, value)
}

// Delete removes a key.
func (r *BatchKeyValueStoreRef[K, V]) Delete(ctx context.Context, key K) error {
	return r.store.Delete(ctx, key)
}

// SetBatch stores multiple key-value pairs efficiently.
func (r *BatchKeyValueStoreRef[K, V]) SetBatch(ctx context.Context, entries []kprocessor.KV[K, V]) error {
	return r.store.SetBatch(ctx, entries)
}

// GetBatch retrieves multiple values by keys.
func (r *BatchKeyValueStoreRef[K, V]) GetBatch(ctx context.Context, keys []K) ([]kprocessor.KV[K, V], error) {
	return r.store.GetBatch(ctx, keys)
}

// DeleteBatch removes multiple keys.
func (r *BatchKeyValueStoreRef[K, V]) DeleteBatch(ctx context.Context, keys []K) error {
	return r.store.DeleteBatch(ctx, keys)
}

// Store returns the underlying store.
func (r *BatchKeyValueStoreRef[K, V]) Store() BatchKeyValueStore[K, V] {
	return r.store
}

// WindowedStoreRef provides type-safe access to a WindowedStore.
type WindowedStoreRef[K comparable, V any] struct {
	store WindowedStore[K, V]
	name  string
}

// Init initializes the windowed store reference from the processor context.
func (r *WindowedStoreRef[K, V]) Init(ctx kprocessor.ProcessorContext[any, any], name string) error {
	store, err := GetWindowedStore[K, V](ctx, name)
	if err != nil {
		return fmt.Errorf("init windowed store ref %q: %w", name, err)
	}
	r.store = store
	r.name = name
	return nil
}

// InitWindowedStoreRef initializes using any ProcessorContext type.
func InitWindowedStoreRef[K comparable, V any, Kout, Vout any](
	r *WindowedStoreRef[K, V],
	ctx kprocessor.ProcessorContext[Kout, Vout],
	name string,
) error {
	store, err := GetWindowedStore[K, V](ctx, name)
	if err != nil {
		return fmt.Errorf("init windowed store ref %q: %w", name, err)
	}
	r.store = store
	r.name = name
	return nil
}

// Name returns the store name.
func (r *WindowedStoreRef[K, V]) Name() string {
	return r.name
}

// Store returns the underlying store.
func (r *WindowedStoreRef[K, V]) Store() WindowedStore[K, V] {
	return r.store
}
