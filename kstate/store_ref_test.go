package kstate

import (
	"context"
	"iter"
	"testing"

	"github.com/alecthomas/assert/v2"
	"github.com/birdayz/kstreams/kprocessor"
)

// mockKeyValueStoreForRef implements KeyValueStore with actual storage
type mockKeyValueStoreForRef[K comparable, V any] struct {
	name string
	data map[K]V
}

func newMockKeyValueStoreForRef[K comparable, V any](name string) *mockKeyValueStoreForRef[K, V] {
	return &mockKeyValueStoreForRef[K, V]{
		name: name,
		data: make(map[K]V),
	}
}

func (m *mockKeyValueStoreForRef[K, V]) Name() string                                       { return m.name }
func (m *mockKeyValueStoreForRef[K, V]) Init(ctx kprocessor.ProcessorContextInternal) error { return nil }
func (m *mockKeyValueStoreForRef[K, V]) Flush(ctx context.Context) error                    { return nil }
func (m *mockKeyValueStoreForRef[K, V]) Close() error                                       { return nil }
func (m *mockKeyValueStoreForRef[K, V]) Persistent() bool                                   { return false }

func (m *mockKeyValueStoreForRef[K, V]) Get(ctx context.Context, key K) (V, bool, error) {
	v, ok := m.data[key]
	return v, ok, nil
}

func (m *mockKeyValueStoreForRef[K, V]) Set(ctx context.Context, key K, value V) error {
	m.data[key] = value
	return nil
}

func (m *mockKeyValueStoreForRef[K, V]) Delete(ctx context.Context, key K) error {
	delete(m.data, key)
	return nil
}

func (m *mockKeyValueStoreForRef[K, V]) Range(ctx context.Context, from, to K) iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {}
}

func (m *mockKeyValueStoreForRef[K, V]) All(ctx context.Context) iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {}
}

// refStoreAdapter wraps a StateStore for testing
type refStoreAdapter struct {
	StateStore
}

func (a *refStoreAdapter) Init() error  { return nil }
func (a *refStoreAdapter) Flush() error { return a.StateStore.Flush(context.Background()) }
func (a *refStoreAdapter) Close() error { return a.StateStore.Close() }

// GetStateStore implements kstate.StateStoreGetter
func (a *refStoreAdapter) GetStateStore() StateStore { return a.StateStore }

func TestKeyValueStoreRef(t *testing.T) {
	t.Run("init and use store ref", func(t *testing.T) {
		// Create mock store with adapter
		kvStore := newMockKeyValueStoreForRef[string, int64]("counter-store")
		adapted := &refStoreAdapter{kvStore}
		ctx := &mockProcessorContext[any, any]{
			stores: map[string]kprocessor.Store{
				"counter-store": adapted,
			},
		}

		// Create and init store ref
		var ref KeyValueStoreRef[string, int64]
		err := ref.Init(ctx, "counter-store")
		assert.NoError(t, err)
		assert.Equal(t, "counter-store", ref.Name())

		// Use the store through the ref
		bgCtx := context.Background()

		// Initially no value
		val, found, err := ref.Get(bgCtx, "key1")
		assert.NoError(t, err)
		assert.False(t, found)
		assert.Equal(t, int64(0), val)

		// Set value
		err = ref.Set(bgCtx, "key1", int64(42))
		assert.NoError(t, err)

		// Get value back
		val, found, err = ref.Get(bgCtx, "key1")
		assert.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, int64(42), val)

		// Delete value
		err = ref.Delete(bgCtx, "key1")
		assert.NoError(t, err)

		// Verify deleted
		_, found, err = ref.Get(bgCtx, "key1")
		assert.NoError(t, err)
		assert.False(t, found)
	})

	t.Run("init fails when store not found", func(t *testing.T) {
		ctx := &mockProcessorContext[any, any]{
			stores: map[string]kprocessor.Store{},
		}

		var ref KeyValueStoreRef[string, int64]
		err := ref.Init(ctx, "nonexistent")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("init fails when wrong type", func(t *testing.T) {
		// Store with different types
		kvStore := newMockKeyValueStoreForRef[int, string]("wrong-type-store")
		adapted := &refStoreAdapter{kvStore}
		ctx := &mockProcessorContext[any, any]{
			stores: map[string]kprocessor.Store{
				"wrong-type-store": adapted,
			},
		}

		var ref KeyValueStoreRef[string, int64]
		err := ref.Init(ctx, "wrong-type-store")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "is not KeyValueStore")
	})

	t.Run("InitKeyValueStoreRef with typed context", func(t *testing.T) {
		kvStore := newMockKeyValueStoreForRef[string, int64]("typed-store")
		adapted := &refStoreAdapter{kvStore}
		ctx := &mockProcessorContext[string, string]{
			stores: map[string]kprocessor.Store{
				"typed-store": adapted,
			},
		}

		var ref KeyValueStoreRef[string, int64]
		err := InitKeyValueStoreRef(&ref, ctx, "typed-store")
		assert.NoError(t, err)
		assert.Equal(t, "typed-store", ref.Name())
	})

	t.Run("store returns underlying store", func(t *testing.T) {
		kvStore := newMockKeyValueStoreForRef[string, int64]("underlying-store")
		adapted := &refStoreAdapter{kvStore}
		ctx := &mockProcessorContext[any, any]{
			stores: map[string]kprocessor.Store{
				"underlying-store": adapted,
			},
		}

		var ref KeyValueStoreRef[string, int64]
		err := ref.Init(ctx, "underlying-store")
		assert.NoError(t, err)
		assert.NotZero(t, ref.Store())
	})
}

// Example of how a processor would use KeyValueStoreRef
type ExampleCounterProcessor struct {
	// Type-safe store declaration as field
	counter KeyValueStoreRef[string, int64]
}

func (p *ExampleCounterProcessor) Init(ctx kprocessor.ProcessorContext[string, int64]) error {
	// One-time init converts interface{} to typed ref
	return InitKeyValueStoreRef(&p.counter, ctx, "counter-store")
}

func (p *ExampleCounterProcessor) Process(ctx context.Context, key string, value int) error {
	// Type-safe operations - no casting needed!
	count, _, _ := p.counter.Get(ctx, key)
	return p.counter.Set(ctx, key, count+int64(value))
}
