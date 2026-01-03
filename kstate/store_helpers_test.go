package kstate

import (
	"context"
	"iter"
	"testing"

	"github.com/alecthomas/assert/v2"
	"github.com/birdayz/kstreams/kprocessor"
)

// Mock processor context for testing
type mockProcessorContext[Kout, Vout any] struct {
	stores map[string]kprocessor.Store
}

func (m *mockProcessorContext[Kout, Vout]) Forward(ctx context.Context, k Kout, v Vout) {}
func (m *mockProcessorContext[Kout, Vout]) ForwardTo(ctx context.Context, k Kout, v Vout, childName string) {
}
func (m *mockProcessorContext[Kout, Vout]) GetStore(name string) kprocessor.Store {
	return m.stores[name]
}

// mockKeyValueStore implements kstate.KeyValueStore[K, V]
type mockKeyValueStore[K comparable, V any] struct {
	name string
}

func (m *mockKeyValueStore[K, V]) Name() string                                       { return m.name }
func (m *mockKeyValueStore[K, V]) Init(ctx kprocessor.ProcessorContextInternal) error { return nil }
func (m *mockKeyValueStore[K, V]) Flush(ctx context.Context) error                    { return nil }
func (m *mockKeyValueStore[K, V]) Close() error                                       { return nil }
func (m *mockKeyValueStore[K, V]) Persistent() bool                                   { return false }
func (m *mockKeyValueStore[K, V]) Get(ctx context.Context, key K) (V, bool, error) {
	var v V
	return v, false, nil
}
func (m *mockKeyValueStore[K, V]) Set(ctx context.Context, key K, value V) error { return nil }
func (m *mockKeyValueStore[K, V]) Delete(ctx context.Context, key K) error       { return nil }
func (m *mockKeyValueStore[K, V]) Range(ctx context.Context, from, to K) iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {}
}
func (m *mockKeyValueStore[K, V]) All(ctx context.Context) iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {}
}

// mockBatchKeyValueStore implements kstate.BatchKeyValueStore[K, V]
type mockBatchKeyValueStore[K comparable, V any] struct {
	mockKeyValueStore[K, V]
}

func (m *mockBatchKeyValueStore[K, V]) SetBatch(ctx context.Context, entries []kprocessor.KV[K, V]) error {
	return nil
}
func (m *mockBatchKeyValueStore[K, V]) GetBatch(ctx context.Context, keys []K) ([]kprocessor.KV[K, V], error) {
	return nil, nil
}
func (m *mockBatchKeyValueStore[K, V]) DeleteBatch(ctx context.Context, keys []K) error { return nil }

// storeAdapter wraps a StateStore to implement kprocessor.Store
// This matches the adapter pattern used in topology_task_builder.go
type storeAdapter struct {
	StateStore
}

func (a *storeAdapter) Init() error  { return nil }
func (a *storeAdapter) Flush() error { return a.StateStore.Flush(context.Background()) }
func (a *storeAdapter) Close() error { return a.StateStore.Close() }

// GetStateStore implements kstate.StateStoreGetter
func (a *storeAdapter) GetStateStore() StateStore { return a.StateStore }

func TestGetKeyValueStore(t *testing.T) {
	t.Run("returns store when found and correct type", func(t *testing.T) {
		kvStore := &mockKeyValueStore[string, int64]{name: "test-store"}
		adapted := &storeAdapter{kvStore}
		ctx := &mockProcessorContext[string, string]{
			stores: map[string]kprocessor.Store{
				"test-store": adapted,
			},
		}

		result, err := GetKeyValueStore[string, int64](ctx, "test-store")
		assert.NoError(t, err)
		assert.NotZero(t, result)
		assert.Equal(t, "test-store", result.Name())
	})

	t.Run("returns error when store not found", func(t *testing.T) {
		ctx := &mockProcessorContext[string, string]{
			stores: map[string]kprocessor.Store{},
		}

		result, err := GetKeyValueStore[string, int64](ctx, "nonexistent")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
		assert.Zero(t, result)
	})

	t.Run("returns error when wrong key type", func(t *testing.T) {
		kvStore := &mockKeyValueStore[int, string]{name: "test-store"}
		adapted := &storeAdapter{kvStore}
		ctx := &mockProcessorContext[string, string]{
			stores: map[string]kprocessor.Store{
				"test-store": adapted,
			},
		}

		// Try to get with wrong key type (string instead of int)
		result, err := GetKeyValueStore[string, string](ctx, "test-store")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "is not KeyValueStore")
		assert.Zero(t, result)
	})

	t.Run("returns error when wrong value type", func(t *testing.T) {
		kvStore := &mockKeyValueStore[string, int64]{name: "test-store"}
		adapted := &storeAdapter{kvStore}
		ctx := &mockProcessorContext[string, string]{
			stores: map[string]kprocessor.Store{
				"test-store": adapted,
			},
		}

		// Try to get with wrong value type (string instead of int64)
		result, err := GetKeyValueStore[string, string](ctx, "test-store")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "is not KeyValueStore")
		assert.Zero(t, result)
	})
}

func TestMustGetKeyValueStore(t *testing.T) {
	t.Run("returns store when found", func(t *testing.T) {
		kvStore := &mockKeyValueStore[string, int64]{name: "test-store"}
		adapted := &storeAdapter{kvStore}
		ctx := &mockProcessorContext[string, string]{
			stores: map[string]kprocessor.Store{
				"test-store": adapted,
			},
		}

		result := MustGetKeyValueStore[string, int64](ctx, "test-store")
		assert.NotZero(t, result)
		assert.Equal(t, "test-store", result.Name())
	})

	t.Run("panics when store not found", func(t *testing.T) {
		ctx := &mockProcessorContext[string, string]{
			stores: map[string]kprocessor.Store{},
		}

		defer func() {
			r := recover()
			assert.NotZero(t, r)
		}()

		MustGetKeyValueStore[string, int64](ctx, "nonexistent")
		t.Fatal("should have panicked")
	})
}

func TestGetBatchKeyValueStore(t *testing.T) {
	t.Run("returns batch store when found and correct type", func(t *testing.T) {
		batchStore := &mockBatchKeyValueStore[string, int64]{
			mockKeyValueStore: mockKeyValueStore[string, int64]{name: "test-store"},
		}
		adapted := &storeAdapter{batchStore}
		ctx := &mockProcessorContext[string, string]{
			stores: map[string]kprocessor.Store{
				"test-store": adapted,
			},
		}

		result, err := GetBatchKeyValueStore[string, int64](ctx, "test-store")
		assert.NoError(t, err)
		assert.NotZero(t, result)
	})

	t.Run("returns error when store is not batch store", func(t *testing.T) {
		// Regular KeyValueStore, not BatchKeyValueStore
		kvStore := &mockKeyValueStore[string, int64]{name: "test-store"}
		adapted := &storeAdapter{kvStore}
		ctx := &mockProcessorContext[string, string]{
			stores: map[string]kprocessor.Store{
				"test-store": adapted,
			},
		}

		result, err := GetBatchKeyValueStore[string, int64](ctx, "test-store")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "is not BatchKeyValueStore")
		assert.Zero(t, result)
	})
}

func TestMustGetBatchKeyValueStore(t *testing.T) {
	t.Run("returns batch store when found", func(t *testing.T) {
		batchStore := &mockBatchKeyValueStore[string, int64]{
			mockKeyValueStore: mockKeyValueStore[string, int64]{name: "test-store"},
		}
		adapted := &storeAdapter{batchStore}
		ctx := &mockProcessorContext[string, string]{
			stores: map[string]kprocessor.Store{
				"test-store": adapted,
			},
		}

		result := MustGetBatchKeyValueStore[string, int64](ctx, "test-store")
		assert.NotZero(t, result)
	})

	t.Run("panics when not batch store", func(t *testing.T) {
		kvStore := &mockKeyValueStore[string, int64]{name: "test-store"}
		adapted := &storeAdapter{kvStore}
		ctx := &mockProcessorContext[string, string]{
			stores: map[string]kprocessor.Store{
				"test-store": adapted,
			},
		}

		defer func() {
			r := recover()
			assert.NotZero(t, r)
		}()

		MustGetBatchKeyValueStore[string, int64](ctx, "test-store")
		t.Fatal("should have panicked")
	})
}
