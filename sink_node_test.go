package kstreams

import (
	"context"
	"errors"
	"testing"

	"github.com/alecthomas/assert/v2"
)

func TestNewSinkNode(t *testing.T) {
	t.Run("creates sink node with provided parameters", func(t *testing.T) {
		topic := "output-topic"
		keySer := func(k string) ([]byte, error) { return []byte(k), nil }
		valueSer := func(v string) ([]byte, error) { return []byte(v), nil }

		sink := NewSinkNode[string, string](nil, topic, keySer, valueSer)

		assert.NotZero(t, sink)
		assert.Equal(t, topic, sink.topic)
		assert.NotZero(t, sink.KeySerializer)
		assert.NotZero(t, sink.ValueSerializer)
		assert.Equal(t, 0, len(sink.futures))
	})

	t.Run("creates sink with different types", func(t *testing.T) {
		keySer := func(k int) ([]byte, error) { return []byte{byte(k)}, nil }
		valueSer := func(v float64) ([]byte, error) { return []byte{byte(v)}, nil }

		sink := NewSinkNode[int, float64](nil, "topic", keySer, valueSer)

		assert.NotZero(t, sink)
		assert.Equal(t, "topic", sink.topic)
	})
}

func TestSinkNode_Serialization(t *testing.T) {
	t.Run("key serialization error is returned", func(t *testing.T) {
		keySer := func(k string) ([]byte, error) {
			return nil, errors.New("key serialization failed")
		}
		valueSer := func(v string) ([]byte, error) { return []byte(v), nil }

		sink := NewSinkNode[string, string](nil, "output-topic", keySer, valueSer)

		err := sink.Process(context.Background(), "key", "value")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to marshal key")
		assert.Contains(t, err.Error(), "key serialization failed")
	})

	t.Run("value serialization error is returned", func(t *testing.T) {
		keySer := func(k string) ([]byte, error) { return []byte(k), nil }
		valueSer := func(v string) ([]byte, error) {
			return nil, errors.New("value serialization failed")
		}

		sink := NewSinkNode[string, string](nil, "output-topic", keySer, valueSer)

		err := sink.Process(context.Background(), "key", "value")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to marshal value")
		assert.Contains(t, err.Error(), "value serialization failed")
	})

	// Note: We cannot test Process() without a real Kafka client
	// as it will panic on nil client. We test serialization errors above
	// and the rest is covered by integration tests.
}

func TestSinkNode_Flush(t *testing.T) {
	t.Run("flush with no pending records", func(t *testing.T) {
		keySer := func(k string) ([]byte, error) { return []byte(k), nil }
		valueSer := func(v string) ([]byte, error) { return []byte(v), nil }

		sink := NewSinkNode[string, string](nil, "output-topic", keySer, valueSer)

		// Flush without processing any records
		err := sink.Flush(context.Background())
		assert.NoError(t, err)
	})

	t.Run("flush clears futures slice", func(t *testing.T) {
		keySer := func(k string) ([]byte, error) { return []byte(k), nil }
		valueSer := func(v string) ([]byte, error) { return []byte(v), nil }

		sink := NewSinkNode[string, string](nil, "output-topic", keySer, valueSer)

		// Manually add a future (simulating what Process does)
		sink.futures = append(sink.futures, produceResult{
			record: nil,
			err:    nil,
		})

		assert.Equal(t, 1, len(sink.futures))

		err := sink.Flush(context.Background())
		assert.NoError(t, err)

		// Futures should be cleared but memory retained
		assert.Equal(t, 0, len(sink.futures))
	})

	t.Run("flush returns error from futures", func(t *testing.T) {
		keySer := func(k string) ([]byte, error) { return []byte(k), nil }
		valueSer := func(v string) ([]byte, error) { return []byte(v), nil }

		sink := NewSinkNode[string, string](nil, "output-topic", keySer, valueSer)

		// Manually add a future with error
		produceErr := errors.New("produce failed")
		sink.futures = append(sink.futures, produceResult{
			record: nil,
			err:    produceErr,
		})

		err := sink.Flush(context.Background())
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to produce")
		assert.Contains(t, err.Error(), "produce failed")
	})

	t.Run("flush returns first error when multiple errors", func(t *testing.T) {
		keySer := func(k string) ([]byte, error) { return []byte(k), nil }
		valueSer := func(v string) ([]byte, error) { return []byte(v), nil }

		sink := NewSinkNode[string, string](nil, "output-topic", keySer, valueSer)

		// Add multiple futures with errors
		err1 := errors.New("error 1")
		err2 := errors.New("error 2")

		sink.futures = append(sink.futures, produceResult{err: err1})
		sink.futures = append(sink.futures, produceResult{err: err2})

		err := sink.Flush(context.Background())
		assert.Error(t, err)
		// Should return the first error encountered
		assert.Contains(t, err.Error(), "error 1")
	})

	t.Run("multiple flushes work correctly", func(t *testing.T) {
		keySer := func(k string) ([]byte, error) { return []byte(k), nil }
		valueSer := func(v string) ([]byte, error) { return []byte(v), nil }

		sink := NewSinkNode[string, string](nil, "output-topic", keySer, valueSer)

		// First flush
		sink.futures = append(sink.futures, produceResult{})
		err := sink.Flush(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, 0, len(sink.futures))

		// Second flush
		sink.futures = append(sink.futures, produceResult{})
		sink.futures = append(sink.futures, produceResult{})
		err = sink.Flush(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, 0, len(sink.futures))
	})
}

func TestSinkNode_Topic(t *testing.T) {
	t.Run("correct topic is set", func(t *testing.T) {
		tests := []string{
			"simple-topic",
			"topic-with-dashes",
			"topic.with.dots",
			"topic_with_underscores",
			"UPPERCASE-TOPIC",
			"topic123",
		}

		for _, topic := range tests {
			keySer := func(k string) ([]byte, error) { return []byte(k), nil }
			valueSer := func(v string) ([]byte, error) { return []byte(v), nil }

			sink := NewSinkNode[string, string](nil, topic, keySer, valueSer)

			assert.Equal(t, topic, sink.topic)
		}
	})
}

func TestSinkNode_Interfaces(t *testing.T) {
	t.Run("implements InputProcessor interface", func(t *testing.T) {
		keySer := func(k string) ([]byte, error) { return []byte(k), nil }
		valueSer := func(v string) ([]byte, error) { return []byte(v), nil }

		sink := NewSinkNode[string, string](nil, "topic", keySer, valueSer)

		// Verify it implements InputProcessor
		var _ InputProcessor[string, string] = sink
	})

	t.Run("implements Flusher interface", func(t *testing.T) {
		keySer := func(k string) ([]byte, error) { return []byte(k), nil }
		valueSer := func(v string) ([]byte, error) { return []byte(v), nil }

		sink := NewSinkNode[string, string](nil, "topic", keySer, valueSer)

		// Verify it implements Flusher
		var _ Flusher = sink
	})
}
