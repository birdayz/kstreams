package processors

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
	"github.com/birdayz/kstreams"
)

func TestWindowKey(t *testing.T) {
	t.Run("creates window key with key and time", func(t *testing.T) {
		now := time.Now()
		wk := WindowKey[string]{
			Key:  "test-key",
			Time: now,
		}

		assert.Equal(t, "test-key", wk.Key)
		assert.Equal(t, now, wk.Time)
	})

	t.Run("different types for key", func(t *testing.T) {
		// String key
		wk1 := WindowKey[string]{Key: "key", Time: time.Now()}
		assert.Equal(t, "key", wk1.Key)

		// Int key
		wk2 := WindowKey[int]{Key: 42, Time: time.Now()}
		assert.Equal(t, 42, wk2.Key)

		// Struct key
		type CustomKey struct {
			ID int
		}
		wk3 := WindowKey[CustomKey]{Key: CustomKey{ID: 1}, Time: time.Now()}
		assert.Equal(t, 1, wk3.Key.ID)
	})
}

func TestWindowedAggregator(t *testing.T) {
	t.Run("basic windowed aggregation", func(t *testing.T) {
		windowSize := time.Minute

		// Timestamp extractor - use current time for simplicity
		timestampExtractor := func(k string, v int) time.Time {
			return time.Now()
		}

		// Init function - start with 0
		initFunc := func() int {
			return 0
		}

		// Aggregate function - sum values
		aggregateFunc := func(value int, state int) int {
			return state + value
		}

		// Finalize function - return the state as-is
		finalizeFunc := func(state int) int {
			return state
		}

		// Create mock store backend
		storeBackend := &mockStoreBackend{
			data: make(map[string][]byte),
		}

		storeBackendBuilder := func(name string, p int32) (kstreams.StoreBackend, error) {
			return storeBackend, nil
		}

		// Placeholder serdes (not actually used in this test)
		keySerde := kstreams.SerDe[string]{
			Serializer:   func(s string) ([]byte, error) { return []byte(s), nil },
			Deserializer: func(b []byte) (string, error) { return string(b), nil },
		}
		stateSerde := kstreams.SerDe[int]{
			Serializer:   func(i int) ([]byte, error) { return []byte{byte(i)}, nil },
			Deserializer: func(b []byte) (int, error) { return int(b[0]), nil },
		}

		processorBuilder, storeBuilder := NewWindowedAggregator(
			timestampExtractor,
			windowSize,
			initFunc,
			aggregateFunc,
			finalizeFunc,
			storeBackendBuilder,
			keySerde,
			stateSerde,
			"test-store",
		)

		assert.NotZero(t, processorBuilder)
		assert.NotZero(t, storeBuilder)

		// Create processor
		processor := processorBuilder()
		assert.NotZero(t, processor)
	})

	t.Run("aggregator processes values into windows", func(t *testing.T) {
		windowSize := time.Minute

		baseTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)

		timestampExtractor := func(k string, v int) time.Time {
			// First 3 records in first window, next 2 in second window
			if v <= 3 {
				return baseTime
			}
			return baseTime.Add(time.Minute)
		}

		initFunc := func() int { return 0 }
		aggregateFunc := func(value int, state int) int { return state + value }
		finalizeFunc := func(state int) int { return state }

		// Create a real WindowedKeyValueStore with mock backend
		backend := &mockStoreBackend{data: make(map[string][]byte)}

		keySer := func(k string) ([]byte, error) { return []byte(k), nil }
		valSer := func(v int) ([]byte, error) { return []byte{byte(v)}, nil }
		keyDeser := func(b []byte) (string, error) { return string(b), nil }
		valDeser := func(b []byte) (int, error) {
			if len(b) == 0 {
				return 0, nil
			}
			return int(b[0]), nil
		}

		windowedStore := NewWindowedKeyValueStore[string, int](backend, keySer, valSer, keyDeser, valDeser)

		// Mock context
		mockCtx := &mockWindowedProcessorContext[string, int]{
			store: windowedStore,
		}

		// Create aggregator directly
		aggregator := &WindowedAggregator[string, int, int, int]{
			store:              windowedStore,
			timestampExtractor: timestampExtractor,
			windowSize:         windowSize,
			initFunc:           initFunc,
			aggregateFunc:      aggregateFunc,
			finalizeFunc:       finalizeFunc,
			storeName:          "test-store",
			processorContext:   mockCtx,
		}

		// Process values
		// Window 1: values 1, 2, 3 = sum 6
		aggregator.Process(context.Background(), "key1", 1)
		aggregator.Process(context.Background(), "key1", 2)
		aggregator.Process(context.Background(), "key1", 3)

		// Window 2: values 4, 5 = sum 9
		aggregator.Process(context.Background(), "key1", 4)
		aggregator.Process(context.Background(), "key1", 5)

		// Verify forwarded records
		assert.Equal(t, 5, len(mockCtx.forwardedRecords))

		// First window should accumulate to 1, 3, 6
		assert.Equal(t, 1, mockCtx.forwardedRecords[0][1])
		assert.Equal(t, 3, mockCtx.forwardedRecords[1][1])
		assert.Equal(t, 6, mockCtx.forwardedRecords[2][1])

		// Second window should accumulate to 4, 9
		assert.Equal(t, 4, mockCtx.forwardedRecords[3][1])
		assert.Equal(t, 9, mockCtx.forwardedRecords[4][1])
	})

	t.Run("different keys have separate aggregations", func(t *testing.T) {
		windowSize := time.Minute
		baseTime := time.Now()

		timestampExtractor := func(k string, v int) time.Time {
			return baseTime
		}

		initFunc := func() int { return 0 }
		aggregateFunc := func(value int, state int) int { return state + value }
		finalizeFunc := func(state int) int { return state }

		backend := &mockStoreBackend{data: make(map[string][]byte)}
		keySer := func(k string) ([]byte, error) { return []byte(k), nil }
		valSer := func(v int) ([]byte, error) { return []byte{byte(v)}, nil }
		keyDeser := func(b []byte) (string, error) { return string(b), nil }
		valDeser := func(b []byte) (int, error) {
			if len(b) == 0 {
				return 0, nil
			}
			return int(b[0]), nil
		}

		windowedStore := NewWindowedKeyValueStore[string, int](backend, keySer, valSer, keyDeser, valDeser)

		mockCtx := &mockWindowedProcessorContext[string, int]{
			store: windowedStore,
		}

		aggregator := &WindowedAggregator[string, int, int, int]{
			store:              windowedStore,
			timestampExtractor: timestampExtractor,
			windowSize:         windowSize,
			initFunc:           initFunc,
			aggregateFunc:      aggregateFunc,
			finalizeFunc:       finalizeFunc,
			storeName:          "test-store",
			processorContext:   mockCtx,
		}

		// Process values for different keys
		aggregator.Process(context.Background(), "key1", 10)
		aggregator.Process(context.Background(), "key2", 20)
		aggregator.Process(context.Background(), "key1", 5)
		aggregator.Process(context.Background(), "key2", 30)

		// Verify separate aggregations
		assert.Equal(t, 4, len(mockCtx.forwardedRecords))

		// Find results for each key
		key1Total := 0
		key2Total := 0
		for _, record := range mockCtx.forwardedRecords {
			wk := record[0].(WindowKey[string])
			value := record[1].(int)

			if wk.Key == "key1" {
				key1Total = value
			} else if wk.Key == "key2" {
				key2Total = value
			}
		}

		assert.Equal(t, 15, key1Total) // 10 + 5
		assert.Equal(t, 50, key2Total) // 20 + 30
	})

	t.Run("window time truncation", func(t *testing.T) {
		windowSize := time.Hour

		// Times within the same hour window
		time1 := time.Date(2024, 1, 1, 12, 15, 0, 0, time.UTC)
		time2 := time.Date(2024, 1, 1, 12, 45, 0, 0, time.UTC)
		expectedWindow := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)

		timestampExtractor := func(k string, v time.Time) time.Time {
			return v
		}

		initFunc := func() int { return 0 }
		aggregateFunc := func(value time.Time, state int) int { return state + 1 }
		finalizeFunc := func(state int) int { return state }

		backend := &mockStoreBackend{data: make(map[string][]byte)}
		keySer := func(k string) ([]byte, error) { return []byte(k), nil }
		valSer := func(v int) ([]byte, error) { return []byte{byte(v)}, nil }
		keyDeser := func(b []byte) (string, error) { return string(b), nil }
		valDeser := func(b []byte) (int, error) {
			if len(b) == 0 {
				return 0, nil
			}
			return int(b[0]), nil
		}

		windowedStore := NewWindowedKeyValueStore[string, int](backend, keySer, valSer, keyDeser, valDeser)

		mockCtx := &mockWindowedProcessorContext[string, int]{
			store: windowedStore,
		}

		aggregator := &WindowedAggregator[string, time.Time, int, int]{
			store:              windowedStore,
			timestampExtractor: timestampExtractor,
			windowSize:         windowSize,
			initFunc:           initFunc,
			aggregateFunc:      aggregateFunc,
			finalizeFunc:       finalizeFunc,
			storeName:          "test-store",
			processorContext:   mockCtx,
		}

		// Process both times
		aggregator.Process(context.Background(), "key", time1)
		aggregator.Process(context.Background(), "key", time2)

		// Both should be in same window
		assert.Equal(t, 2, len(mockCtx.forwardedRecords))

		// Verify window time is truncated
		wk1 := mockCtx.forwardedRecords[0][0].(WindowKey[string])
		wk2 := mockCtx.forwardedRecords[1][0].(WindowKey[string])

		assert.Equal(t, expectedWindow, wk1.Time)
		assert.Equal(t, expectedWindow, wk2.Time)

		// Second record should show accumulated count
		assert.Equal(t, 2, mockCtx.forwardedRecords[1][1])
	})

	t.Run("init and close lifecycle", func(t *testing.T) {
		windowSize := time.Minute
		timestampExtractor := func(k string, v int) time.Time { return time.Now() }
		initFunc := func() int { return 0 }
		aggregateFunc := func(value int, state int) int { return state + value }
		finalizeFunc := func(state int) int { return state }

		backend := &mockStoreBackend{data: make(map[string][]byte)}
		keySer := func(k string) ([]byte, error) { return []byte(k), nil }
		valSer := func(v int) ([]byte, error) { return []byte{byte(v)}, nil }
		keyDeser := func(b []byte) (string, error) { return string(b), nil }
		valDeser := func(b []byte) (int, error) {
			if len(b) == 0 {
				return 0, nil
			}
			return int(b[0]), nil
		}

		windowedStore := NewWindowedKeyValueStore[string, int](backend, keySer, valSer, keyDeser, valDeser)

		aggregator := &WindowedAggregator[string, int, int, int]{
			timestampExtractor: timestampExtractor,
			windowSize:         windowSize,
			initFunc:           initFunc,
			aggregateFunc:      aggregateFunc,
			finalizeFunc:       finalizeFunc,
			storeName:          "test-store",
		}

		// Init with context
		mockCtx := &mockWindowedProcessorContext[string, int]{
			store: windowedStore,
		}

		err := aggregator.Init(mockCtx)
		assert.NoError(t, err)
		assert.NotZero(t, aggregator.store)
		assert.NotZero(t, aggregator.processorContext)

		// Close
		err = aggregator.Close()
		assert.NoError(t, err)
	})
}

// Mock processor context for windowed aggregator
type mockWindowedProcessorContext[K, V any] struct {
	store            *WindowedKeyValueStore[K, V]
	forwardedRecords [][2]any
	mu               sync.Mutex
}

func (m *mockWindowedProcessorContext[K, V]) Forward(ctx context.Context, k WindowKey[K], v V) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.forwardedRecords = append(m.forwardedRecords, [2]any{k, v})
}

func (m *mockWindowedProcessorContext[K, V]) ForwardTo(ctx context.Context, k WindowKey[K], v V, childName string) {
}

func (m *mockWindowedProcessorContext[K, V]) GetStore(name string) kstreams.Store {
	return any(m.store).(kstreams.Store)
}

// Mock store backend for builder tests
type mockStoreBackend struct {
	data map[string][]byte
	mu   sync.Mutex
}

func (m *mockStoreBackend) Init() error  { return nil }
func (m *mockStoreBackend) Flush() error { return nil }
func (m *mockStoreBackend) Close() error { return nil }
func (m *mockStoreBackend) Checkpoint(ctx context.Context, id string) error { return nil }

func (m *mockStoreBackend) Set(k, v []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[string(k)] = v
	return nil
}

func (m *mockStoreBackend) Get(k []byte) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if v, ok := m.data[string(k)]; ok {
		return v, nil
	}
	return nil, kstreams.ErrKeyNotFound
}

func (m *mockStoreBackend) Delete(k []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, string(k))
	return nil
}

func (m *mockStoreBackend) Range(start, end []byte) (kstreams.Iterator, error) {
	return &mockIterator{}, nil
}

func (m *mockStoreBackend) All() (kstreams.Iterator, error) {
	return &mockIterator{}, nil
}

func (m *mockStoreBackend) ReverseRange(start, end []byte) (kstreams.Iterator, error) {
	return &mockIterator{}, nil
}

func (m *mockStoreBackend) ReverseAll() (kstreams.Iterator, error) {
	return &mockIterator{}, nil
}

type mockIterator struct{}

func (m *mockIterator) Next() bool         { return false }
func (m *mockIterator) Key() []byte        { return nil }
func (m *mockIterator) Value() []byte      { return nil }
func (m *mockIterator) Err() error         { return nil }
func (m *mockIterator) Close() error       { return nil }
