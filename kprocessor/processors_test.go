package kprocessor

import (
	"context"
	"sync"
	"testing"

	"github.com/alecthomas/assert/v2"
)

func TestForEach(t *testing.T) {
	t.Run("processes each record", func(t *testing.T) {
		var processedKeys []string
		var processedValues []string
		var mu sync.Mutex

		forEachFunc := func(k string, v string) {
			mu.Lock()
			defer mu.Unlock()
			processedKeys = append(processedKeys, k)
			processedValues = append(processedValues, v)
		}

		builder := ForEach(forEachFunc)
		processor := builder()

		// Init processor
		ctx := &mockProcessorContext[string, string]{}
		err := processor.Init(ctx)
		assert.NoError(t, err)

		// Process records
		assert.NoError(t, processor.Process(context.Background(), "key1", "value1"))
		assert.NoError(t, processor.Process(context.Background(), "key2", "value2"))
		assert.NoError(t, processor.Process(context.Background(), "key3", "value3"))

		// Verify all records were processed
		mu.Lock()
		defer mu.Unlock()
		assert.Equal(t, 3, len(processedKeys))
		assert.Equal(t, []string{"key1", "key2", "key3"}, processedKeys)
		assert.Equal(t, []string{"value1", "value2", "value3"}, processedValues)
	})

	t.Run("handles empty processing", func(t *testing.T) {
		callCount := 0
		forEachFunc := func(k string, v string) {
			callCount++
		}

		builder := ForEach(forEachFunc)
		processor := builder()

		ctx := &mockProcessorContext[string, string]{}
		assert.NoError(t, processor.Init(ctx))

		// Don't process any records
		assert.Equal(t, 0, callCount)

		// Close should work
		err := processor.Close()
		assert.NoError(t, err)
	})

	t.Run("processes different types", func(t *testing.T) {
		type TestKey struct {
			ID   int
			Name string
		}

		type TestValue struct {
			Data string
		}

		var processedKey *TestKey
		var processedValue *TestValue

		forEachFunc := func(k TestKey, v TestValue) {
			processedKey = &k
			processedValue = &v
		}

		builder := ForEach(forEachFunc)
		processor := builder()

		ctx := &mockProcessorContext[TestKey, TestValue]{}
		assert.NoError(t, processor.Init(ctx))

		key := TestKey{ID: 1, Name: "test"}
		value := TestValue{Data: "test-data"}

		assert.NoError(t, processor.Process(context.Background(), key, value))

		assert.NotZero(t, processedKey)
		assert.NotZero(t, processedValue)
		assert.Equal(t, 1, processedKey.ID)
		assert.Equal(t, "test", processedKey.Name)
		assert.Equal(t, "test-data", processedValue.Data)
	})

	t.Run("side effects in forEach function", func(t *testing.T) {
		// Test that forEach can perform side effects
		sideEffectMap := make(map[string]int)
		var mu sync.Mutex

		forEachFunc := func(k string, v string) {
			mu.Lock()
			defer mu.Unlock()
			// Side effect: populate a map
			sideEffectMap[k] = len(v)
		}

		builder := ForEach(forEachFunc)
		processor := builder()

		ctx := &mockProcessorContext[string, string]{}
		assert.NoError(t, processor.Init(ctx))

		assert.NoError(t, processor.Process(context.Background(), "short", "hi"))
		assert.NoError(t, processor.Process(context.Background(), "medium", "hello"))
		assert.NoError(t, processor.Process(context.Background(), "long", "hello world"))

		mu.Lock()
		defer mu.Unlock()
		assert.Equal(t, 3, len(sideEffectMap))
		assert.Equal(t, 2, sideEffectMap["short"])
		assert.Equal(t, 5, sideEffectMap["medium"])
		assert.Equal(t, 11, sideEffectMap["long"])
	})

	t.Run("forEach does not forward records", func(t *testing.T) {
		forEachFunc := func(k string, v string) {
			// Do nothing
		}

		builder := ForEach(forEachFunc)
		processor := builder()

		ctx := &mockProcessorContext[string, string]{}
		assert.NoError(t, processor.Init(ctx))

		assert.NoError(t, processor.Process(context.Background(), "key", "value"))

		// ForEach should not call Forward
		assert.Equal(t, 0, len(ctx.forwardedRecords))
	})

	t.Run("concurrent processing", func(t *testing.T) {
		var processedCount int32
		var mu sync.Mutex

		forEachFunc := func(k int, v int) {
			mu.Lock()
			defer mu.Unlock()
			processedCount++
		}

		builder := ForEach(forEachFunc)
		processor := builder()

		ctx := &mockProcessorContext[int, int]{}
		assert.NoError(t, processor.Init(ctx))

		// Process multiple records concurrently
		var wg sync.WaitGroup
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func(val int) {
				defer wg.Done()
				_ = processor.Process(context.Background(), val, val)
			}(i)
		}

		wg.Wait()

		mu.Lock()
		defer mu.Unlock()
		assert.Equal(t, int32(100), processedCount)
	})

	t.Run("init and close lifecycle", func(t *testing.T) {
		forEachFunc := func(k string, v string) {}

		builder := ForEach(forEachFunc)
		processor := builder()

		// Init
		ctx := &mockProcessorContext[string, string]{}
		err := processor.Init(ctx)
		assert.NoError(t, err)

		// Process
		err = processor.Process(context.Background(), "key", "value")
		assert.NoError(t, err)

		// Close
		err = processor.Close()
		assert.NoError(t, err)
	})
}

func TestFilter(t *testing.T) {
	t.Run("forwards matching records", func(t *testing.T) {
		builder := Filter(func(k string, v int) bool {
			return v > 10
		})
		processor := builder()

		ctx := &mockProcessorContext[string, int]{}
		assert.NoError(t, processor.Init(ctx))

		assert.NoError(t, processor.Process(context.Background(), "a", 5))
		assert.NoError(t, processor.Process(context.Background(), "b", 15))
		assert.NoError(t, processor.Process(context.Background(), "c", 8))
		assert.NoError(t, processor.Process(context.Background(), "d", 20))

		assert.Equal(t, 2, len(ctx.forwardedRecords))
		assert.Equal(t, "b", ctx.forwardedRecords[0][0])
		assert.Equal(t, 15, ctx.forwardedRecords[0][1])
		assert.Equal(t, "d", ctx.forwardedRecords[1][0])
		assert.Equal(t, 20, ctx.forwardedRecords[1][1])
	})
}

func TestMap(t *testing.T) {
	t.Run("transforms values", func(t *testing.T) {
		builder := Map(func(k string, v string) int {
			return len(v)
		})
		processor := builder()

		ctx := &mockProcessorContext[string, int]{}
		assert.NoError(t, processor.Init(ctx))

		assert.NoError(t, processor.Process(context.Background(), "a", "hello"))
		assert.NoError(t, processor.Process(context.Background(), "b", "hi"))

		assert.Equal(t, 2, len(ctx.forwardedRecords))
		assert.Equal(t, "a", ctx.forwardedRecords[0][0])
		assert.Equal(t, 5, ctx.forwardedRecords[0][1])
		assert.Equal(t, "b", ctx.forwardedRecords[1][0])
		assert.Equal(t, 2, ctx.forwardedRecords[1][1])
	})
}

func TestPeek(t *testing.T) {
	t.Run("peeks and forwards", func(t *testing.T) {
		var peeked []string
		builder := Peek(func(k string, v string) {
			peeked = append(peeked, k+":"+v)
		})
		processor := builder()

		ctx := &mockProcessorContext[string, string]{}
		assert.NoError(t, processor.Init(ctx))

		assert.NoError(t, processor.Process(context.Background(), "a", "1"))
		assert.NoError(t, processor.Process(context.Background(), "b", "2"))

		// Verify peek was called
		assert.Equal(t, []string{"a:1", "b:2"}, peeked)

		// Verify records were forwarded
		assert.Equal(t, 2, len(ctx.forwardedRecords))
	})
}

func TestFlatMap(t *testing.T) {
	t.Run("expands records", func(t *testing.T) {
		builder := FlatMap(func(k string, v string) []KV[string, string] {
			return []KV[string, string]{
				{Key: k + "-1", Value: v},
				{Key: k + "-2", Value: v},
			}
		})
		processor := builder()

		ctx := &mockProcessorContext[string, string]{}
		assert.NoError(t, processor.Init(ctx))

		assert.NoError(t, processor.Process(context.Background(), "key", "val"))

		assert.Equal(t, 2, len(ctx.forwardedRecords))
		assert.Equal(t, "key-1", ctx.forwardedRecords[0][0])
		assert.Equal(t, "key-2", ctx.forwardedRecords[1][0])
	})
}

// Mock processor context for testing
type mockProcessorContext[Kout, Vout any] struct {
	forwardedRecords [][2]any
	mu               sync.Mutex
}

func (m *mockProcessorContext[Kout, Vout]) Forward(ctx context.Context, k Kout, v Vout) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.forwardedRecords = append(m.forwardedRecords, [2]any{k, v})
}

func (m *mockProcessorContext[Kout, Vout]) ForwardTo(ctx context.Context, k Kout, v Vout, childName string) {
}

func (m *mockProcessorContext[Kout, Vout]) GetStore(name string) Store {
	return nil
}
