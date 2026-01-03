package kstate

import (
	"errors"
	"iter"
	"testing"

	"github.com/alecthomas/assert/v2"
)

func TestMapIter(t *testing.T) {
	t.Run("transforms byte iterator to typed iterator", func(t *testing.T) {
		// Create a byte iterator
		byteSeq := func(yield func([]byte, []byte) bool) {
			if !yield([]byte("key1"), []byte("value1")) {
				return
			}
			if !yield([]byte("key2"), []byte("value2")) {
				return
			}
			if !yield([]byte("key3"), []byte("value3")) {
				return
			}
		}

		keyDeser := func(data []byte) (string, error) { return string(data), nil }
		valueDeser := func(data []byte) (string, error) { return string(data), nil }

		typedSeq := MapIter(byteSeq, keyDeser, valueDeser)

		var results []struct{ k, v string }
		for k, v := range typedSeq {
			results = append(results, struct{ k, v string }{k, v})
		}

		assert.Equal(t, 3, len(results))
		assert.Equal(t, "key1", results[0].k)
		assert.Equal(t, "value1", results[0].v)
		assert.Equal(t, "key2", results[1].k)
		assert.Equal(t, "value2", results[1].v)
		assert.Equal(t, "key3", results[2].k)
		assert.Equal(t, "value3", results[2].v)
	})

	t.Run("handles empty iterator", func(t *testing.T) {
		byteSeq := func(yield func([]byte, []byte) bool) {}

		keyDeser := func(data []byte) (string, error) { return string(data), nil }
		valueDeser := func(data []byte) (string, error) { return string(data), nil }

		typedSeq := MapIter(byteSeq, keyDeser, valueDeser)

		count := 0
		for range typedSeq {
			count++
		}
		assert.Equal(t, 0, count)
	})

	t.Run("stops on key deserialization error", func(t *testing.T) {
		byteSeq := func(yield func([]byte, []byte) bool) {
			yield([]byte("key1"), []byte("value1"))
			yield([]byte("key2"), []byte("value2"))
		}

		callCount := 0
		keyDeser := func(data []byte) (string, error) {
			callCount++
			if callCount == 2 {
				return "", errors.New("key deserialization failed")
			}
			return string(data), nil
		}
		valueDeser := func(data []byte) (string, error) { return string(data), nil }

		typedSeq := MapIter(byteSeq, keyDeser, valueDeser)

		count := 0
		for range typedSeq {
			count++
		}
		assert.Equal(t, 1, count) // Only first item before error
	})

	t.Run("stops on value deserialization error", func(t *testing.T) {
		byteSeq := func(yield func([]byte, []byte) bool) {
			yield([]byte("key1"), []byte("value1"))
			yield([]byte("key2"), []byte("value2"))
		}

		keyDeser := func(data []byte) (string, error) { return string(data), nil }
		callCount := 0
		valueDeser := func(data []byte) (string, error) {
			callCount++
			if callCount == 2 {
				return "", errors.New("value deserialization failed")
			}
			return string(data), nil
		}

		typedSeq := MapIter(byteSeq, keyDeser, valueDeser)

		count := 0
		for range typedSeq {
			count++
		}
		assert.Equal(t, 1, count)
	})

	t.Run("supports early break", func(t *testing.T) {
		byteSeq := func(yield func([]byte, []byte) bool) {
			for i := 0; i < 100; i++ {
				if !yield([]byte("key"), []byte("value")) {
					return
				}
			}
		}

		keyDeser := func(data []byte) (string, error) { return string(data), nil }
		valueDeser := func(data []byte) (string, error) { return string(data), nil }

		typedSeq := MapIter(byteSeq, keyDeser, valueDeser)

		count := 0
		for range typedSeq {
			count++
			if count == 5 {
				break
			}
		}
		assert.Equal(t, 5, count)
	})

	t.Run("works with range over func", func(t *testing.T) {
		byteSeq := func(yield func([]byte, []byte) bool) {
			yield([]byte("a"), []byte("1"))
			yield([]byte("b"), []byte("2"))
		}

		keyDeser := func(data []byte) (string, error) { return string(data), nil }
		valueDeser := func(data []byte) (int, error) { return int(data[0] - '0'), nil }

		typedSeq := MapIter(byteSeq, keyDeser, valueDeser)

		sum := 0
		for _, v := range typedSeq {
			sum += v
		}
		assert.Equal(t, 3, sum)
	})
}

// Verify MapIter returns the correct type
var _ iter.Seq2[string, int] = MapIter[string, int](
	func(yield func([]byte, []byte) bool) {},
	func([]byte) (string, error) { return "", nil },
	func([]byte) (int, error) { return 0, nil },
)
