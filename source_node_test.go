package kstreams

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestSourceNode_Process(t *testing.T) {
	t.Run("successfully processes and forwards record", func(t *testing.T) {
		keyDeser := func(data []byte) (string, error) {
			return string(data), nil
		}
		valueDeser := func(data []byte) (string, error) {
			return string(data), nil
		}

		var processedKey string
		var processedValue string
		downstream := &mockInputProcessor[string, string]{
			processFunc: func(ctx context.Context, k string, v string) error {
				processedKey = k
				processedValue = v
				return nil
			},
		}

		source := &SourceNode[string, string]{
			KeyDeserializer:      keyDeser,
			ValueDeserializer:    valueDeser,
			DownstreamProcessors: []InputProcessor[string, string]{downstream},
		}

		record := &kgo.Record{
			Key:   []byte("test-key"),
			Value: []byte("test-value"),
			Topic: "test-topic",
		}

		err := source.Process(context.Background(), record)
		assert.NoError(t, err)
		assert.Equal(t, "test-key", processedKey)
		assert.Equal(t, "test-value", processedValue)
	})

	t.Run("returns error on key deserialization failure", func(t *testing.T) {
		keyDeser := func(data []byte) (string, error) {
			return "", errors.New("key deserialization failed")
		}
		valueDeser := func(data []byte) (string, error) {
			return string(data), nil
		}

		source := &SourceNode[string, string]{
			KeyDeserializer:   keyDeser,
			ValueDeserializer: valueDeser,
		}

		record := &kgo.Record{
			Key:   []byte("test-key"),
			Value: []byte("test-value"),
		}

		err := source.Process(context.Background(), record)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "key deserialization failed")
	})

	t.Run("returns error on value deserialization failure", func(t *testing.T) {
		keyDeser := func(data []byte) (string, error) {
			return string(data), nil
		}
		valueDeser := func(data []byte) (string, error) {
			return "", errors.New("value deserialization failed")
		}

		source := &SourceNode[string, string]{
			KeyDeserializer:   keyDeser,
			ValueDeserializer: valueDeser,
		}

		record := &kgo.Record{
			Key:   []byte("test-key"),
			Value: []byte("test-value"),
		}

		err := source.Process(context.Background(), record)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "value deserialization failed")
	})

	t.Run("forwards to multiple downstream processors", func(t *testing.T) {
		keyDeser := func(data []byte) (string, error) {
			return string(data), nil
		}
		valueDeser := func(data []byte) (string, error) {
			return string(data), nil
		}

		var downstream1Called bool
		var downstream2Called bool

		downstream1 := &mockInputProcessor[string, string]{
			processFunc: func(ctx context.Context, k string, v string) error {
				downstream1Called = true
				return nil
			},
		}
		downstream2 := &mockInputProcessor[string, string]{
			processFunc: func(ctx context.Context, k string, v string) error {
				downstream2Called = true
				return nil
			},
		}

		source := &SourceNode[string, string]{
			KeyDeserializer:      keyDeser,
			ValueDeserializer:    valueDeser,
			DownstreamProcessors: []InputProcessor[string, string]{downstream1, downstream2},
		}

		record := &kgo.Record{
			Key:   []byte("test-key"),
			Value: []byte("test-value"),
		}

		err := source.Process(context.Background(), record)
		assert.NoError(t, err)
		assert.True(t, downstream1Called)
		assert.True(t, downstream2Called)
	})

	t.Run("returns error from downstream processor", func(t *testing.T) {
		keyDeser := func(data []byte) (string, error) {
			return string(data), nil
		}
		valueDeser := func(data []byte) (string, error) {
			return string(data), nil
		}

		downstream := &mockInputProcessor[string, string]{
			processFunc: func(ctx context.Context, k string, v string) error {
				return errors.New("downstream processing failed")
			},
		}

		source := &SourceNode[string, string]{
			KeyDeserializer:      keyDeser,
			ValueDeserializer:    valueDeser,
			DownstreamProcessors: []InputProcessor[string, string]{downstream},
		}

		record := &kgo.Record{
			Key:   []byte("test-key"),
			Value: []byte("test-value"),
		}

		err := source.Process(context.Background(), record)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "downstream processing failed")
	})

	t.Run("processes empty key and value", func(t *testing.T) {
		keyDeser := func(data []byte) (string, error) {
			return string(data), nil
		}
		valueDeser := func(data []byte) (string, error) {
			return string(data), nil
		}

		var processedKey string
		var processedValue string
		downstream := &mockInputProcessor[string, string]{
			processFunc: func(ctx context.Context, k string, v string) error {
				processedKey = k
				processedValue = v
				return nil
			},
		}

		source := &SourceNode[string, string]{
			KeyDeserializer:      keyDeser,
			ValueDeserializer:    valueDeser,
			DownstreamProcessors: []InputProcessor[string, string]{downstream},
		}

		record := &kgo.Record{
			Key:   []byte{},
			Value: []byte{},
		}

		err := source.Process(context.Background(), record)
		assert.NoError(t, err)
		assert.Equal(t, "", processedKey)
		assert.Equal(t, "", processedValue)
	})

	t.Run("processes nil key and value", func(t *testing.T) {
		keyDeser := func(data []byte) (string, error) {
			return string(data), nil
		}
		valueDeser := func(data []byte) (string, error) {
			return string(data), nil
		}

		var processedKey string
		var processedValue string
		downstream := &mockInputProcessor[string, string]{
			processFunc: func(ctx context.Context, k string, v string) error {
				processedKey = k
				processedValue = v
				return nil
			},
		}

		source := &SourceNode[string, string]{
			KeyDeserializer:      keyDeser,
			ValueDeserializer:    valueDeser,
			DownstreamProcessors: []InputProcessor[string, string]{downstream},
		}

		record := &kgo.Record{
			Key:   nil,
			Value: nil,
		}

		err := source.Process(context.Background(), record)
		assert.NoError(t, err)
		assert.Equal(t, "", processedKey)
		assert.Equal(t, "", processedValue)
	})

	t.Run("processes with no downstream processors", func(t *testing.T) {
		keyDeser := func(data []byte) (string, error) {
			return string(data), nil
		}
		valueDeser := func(data []byte) (string, error) {
			return string(data), nil
		}

		source := &SourceNode[string, string]{
			KeyDeserializer:      keyDeser,
			ValueDeserializer:    valueDeser,
			DownstreamProcessors: []InputProcessor[string, string]{},
		}

		record := &kgo.Record{
			Key:   []byte("test-key"),
			Value: []byte("test-value"),
		}

		err := source.Process(context.Background(), record)
		assert.NoError(t, err)
	})

	t.Run("processes different types", func(t *testing.T) {
		// Test with int key and float64 value
		keyDeser := func(data []byte) (int, error) {
			if len(data) == 0 {
				return 0, nil
			}
			return int(data[0]), nil
		}
		valueDeser := func(data []byte) (float64, error) {
			if len(data) == 0 {
				return 0.0, nil
			}
			return float64(data[0]), nil
		}

		var processedKey int
		var processedValue float64
		downstream := &mockInputProcessor[int, float64]{
			processFunc: func(ctx context.Context, k int, v float64) error {
				processedKey = k
				processedValue = v
				return nil
			},
		}

		source := &SourceNode[int, float64]{
			KeyDeserializer:      keyDeser,
			ValueDeserializer:    valueDeser,
			DownstreamProcessors: []InputProcessor[int, float64]{downstream},
		}

		record := &kgo.Record{
			Key:   []byte{42},
			Value: []byte{100},
		}

		err := source.Process(context.Background(), record)
		assert.NoError(t, err)
		assert.Equal(t, 42, processedKey)
		assert.Equal(t, 100.0, processedValue)
	})

	t.Run("respects context cancellation", func(t *testing.T) {
		keyDeser := func(data []byte) (string, error) {
			return string(data), nil
		}
		valueDeser := func(data []byte) (string, error) {
			return string(data), nil
		}

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		downstream := &mockInputProcessor[string, string]{
			processFunc: func(ctx context.Context, k string, v string) error {
				// Check if context is cancelled
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
					return nil
				}
			},
		}

		source := &SourceNode[string, string]{
			KeyDeserializer:      keyDeser,
			ValueDeserializer:    valueDeser,
			DownstreamProcessors: []InputProcessor[string, string]{downstream},
		}

		record := &kgo.Record{
			Key:   []byte("test-key"),
			Value: []byte("test-value"),
		}

		err := source.Process(ctx, record)
		assert.Error(t, err)
		assert.Equal(t, context.Canceled, err)
	})
}

func TestSourceNode_AddNext(t *testing.T) {
	t.Run("adds downstream processor", func(t *testing.T) {
		source := &SourceNode[string, string]{
			DownstreamProcessors: []InputProcessor[string, string]{},
		}

		downstream := &mockInputProcessor[string, string]{}
		source.AddNext(downstream)

		assert.Equal(t, 1, len(source.DownstreamProcessors))
		// Verify it's the same processor by calling Process
		var called bool
		source.DownstreamProcessors[0].Process(context.Background(), "", "")
		_ = called // Just verify it doesn't panic
	})

	t.Run("adds multiple downstream processors", func(t *testing.T) {
		source := &SourceNode[string, string]{
			DownstreamProcessors: []InputProcessor[string, string]{},
		}

		downstream1 := &mockInputProcessor[string, string]{}
		downstream2 := &mockInputProcessor[string, string]{}
		downstream3 := &mockInputProcessor[string, string]{}

		source.AddNext(downstream1)
		source.AddNext(downstream2)
		source.AddNext(downstream3)

		assert.Equal(t, 3, len(source.DownstreamProcessors))
		// Verify all processors were added
		for _, proc := range source.DownstreamProcessors {
			err := proc.Process(context.Background(), "", "")
			assert.NoError(t, err)
		}
	})

	t.Run("preserves order of addition", func(t *testing.T) {
		source := &SourceNode[string, string]{}

		order := []int{}
		for i := 0; i < 5; i++ {
			idx := i
			downstream := &mockInputProcessor[string, string]{
				processFunc: func(ctx context.Context, k string, v string) error {
					order = append(order, idx)
					return nil
				},
			}
			source.AddNext(downstream)
		}

		keyDeser := func(data []byte) (string, error) {
			return string(data), nil
		}
		valueDeser := func(data []byte) (string, error) {
			return string(data), nil
		}
		source.KeyDeserializer = keyDeser
		source.ValueDeserializer = valueDeser

		record := &kgo.Record{
			Key:   []byte("key"),
			Value: []byte("value"),
		}

		source.Process(context.Background(), record)

		// Verify order
		for i := 0; i < 5; i++ {
			assert.Equal(t, i, order[i])
		}
	})
}

func TestSourceNode_Integration(t *testing.T) {
	t.Run("full pipeline with multiple downstream processors", func(t *testing.T) {
		keyDeser := func(data []byte) (string, error) {
			return string(data), nil
		}
		valueDeser := func(data []byte) (string, error) {
			return string(data), nil
		}

		// Create a pipeline: source -> processor1 -> processor2
		var processor1Calls int
		var processor2Calls int

		processor2 := &mockInputProcessor[string, string]{
			processFunc: func(ctx context.Context, k string, v string) error {
				processor2Calls++
				return nil
			},
		}

		processor1 := &mockInputProcessor[string, string]{
			processFunc: func(ctx context.Context, k string, v string) error {
				processor1Calls++
				// Forward to processor2
				return processor2.Process(ctx, k, v)
			},
		}

		source := &SourceNode[string, string]{
			KeyDeserializer:      keyDeser,
			ValueDeserializer:    valueDeser,
			DownstreamProcessors: []InputProcessor[string, string]{processor1},
		}

		// Process multiple records
		for i := 0; i < 10; i++ {
			record := &kgo.Record{
				Key:   []byte("key"),
				Value: []byte("value"),
			}
			err := source.Process(context.Background(), record)
			assert.NoError(t, err)
		}

		assert.Equal(t, 10, processor1Calls)
		assert.Equal(t, 10, processor2Calls)
	})

	t.Run("handles record with metadata", func(t *testing.T) {
		keyDeser := func(data []byte) (string, error) {
			return string(data), nil
		}
		valueDeser := func(data []byte) (string, error) {
			return string(data), nil
		}

		downstream := &mockInputProcessor[string, string]{
			processFunc: func(ctx context.Context, k string, v string) error {
				return nil
			},
		}

		source := &SourceNode[string, string]{
			KeyDeserializer:      keyDeser,
			ValueDeserializer:    valueDeser,
			DownstreamProcessors: []InputProcessor[string, string]{downstream},
		}

		// Record with full metadata
		record := &kgo.Record{
			Key:         []byte("test-key"),
			Value:       []byte("test-value"),
			Topic:       "test-topic",
			Partition:   3,
			Offset:      100,
			LeaderEpoch: 5,
			Timestamp:   time.Now(),
			Headers: []kgo.RecordHeader{
				{Key: "header1", Value: []byte("value1")},
			},
		}

		err := source.Process(context.Background(), record)
		assert.NoError(t, err)
	})
}

// Mock InputProcessor for testing
type mockInputProcessor[K, V any] struct {
	processFunc func(ctx context.Context, k K, v V) error
}

func (m *mockInputProcessor[K, V]) Process(ctx context.Context, k K, v V) error {
	if m.processFunc != nil {
		return m.processFunc(ctx, k, v)
	}
	return nil
}
