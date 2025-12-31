package kstreams

import (
	"errors"
	"testing"

	"github.com/alecthomas/assert/v2"
)

// mockIterator is a test implementation of Iterator
type mockIterator struct {
	keys   [][]byte
	values [][]byte
	index  int
	err    error
}

func (m *mockIterator) Next() bool {
	if m.err != nil {
		return false
	}
	if m.index >= len(m.keys) {
		return false
	}
	m.index++
	return true
}

func (m *mockIterator) Key() []byte {
	if m.index <= 0 || m.index > len(m.keys) {
		return nil
	}
	return m.keys[m.index-1]
}

func (m *mockIterator) Value() []byte {
	if m.index <= 0 || m.index > len(m.values) {
		return nil
	}
	return m.values[m.index-1]
}

func (m *mockIterator) Err() error {
	return m.err
}

func (m *mockIterator) Close() error {
	return m.err
}

func TestTypedIterator_Next(t *testing.T) {
	t.Run("successfully iterates over multiple items", func(t *testing.T) {
		mock := &mockIterator{
			keys:   [][]byte{[]byte("key1"), []byte("key2"), []byte("key3")},
			values: [][]byte{[]byte("value1"), []byte("value2"), []byte("value3")},
			index:  0,
		}

		keyDeser := func(data []byte) (string, error) { return string(data), nil }
		valueDeser := func(data []byte) (string, error) { return string(data), nil }

		iter := NewTypedIterator(mock, keyDeser, valueDeser)

		// First item
		assert.True(t, iter.Next())
		assert.Equal(t, "key1", iter.Key())
		assert.Equal(t, "value1", iter.Value())
		assert.NoError(t, iter.Err())

		// Second item
		assert.True(t, iter.Next())
		assert.Equal(t, "key2", iter.Key())
		assert.Equal(t, "value2", iter.Value())
		assert.NoError(t, iter.Err())

		// Third item
		assert.True(t, iter.Next())
		assert.Equal(t, "key3", iter.Key())
		assert.Equal(t, "value3", iter.Value())
		assert.NoError(t, iter.Err())

		// End of iteration
		assert.False(t, iter.Next())
	})

	t.Run("handles empty iterator", func(t *testing.T) {
		mock := &mockIterator{
			keys:   [][]byte{},
			values: [][]byte{},
			index:  0,
		}

		keyDeser := func(data []byte) (string, error) { return string(data), nil }
		valueDeser := func(data []byte) (string, error) { return string(data), nil }

		iter := NewTypedIterator(mock, keyDeser, valueDeser)

		assert.False(t, iter.Next())
		assert.NoError(t, iter.Err())
	})

	t.Run("returns false on key deserialization error", func(t *testing.T) {
		mock := &mockIterator{
			keys:   [][]byte{[]byte("key1")},
			values: [][]byte{[]byte("value1")},
			index:  0,
		}

		keyDeser := func(data []byte) (string, error) {
			return "", errors.New("key deserialization failed")
		}
		valueDeser := func(data []byte) (string, error) { return string(data), nil }

		iter := NewTypedIterator(mock, keyDeser, valueDeser)

		assert.False(t, iter.Next())
		assert.Error(t, iter.Err())
		assert.Contains(t, iter.Err().Error(), "key deserialization failed")
	})

	t.Run("returns false on value deserialization error", func(t *testing.T) {
		mock := &mockIterator{
			keys:   [][]byte{[]byte("key1")},
			values: [][]byte{[]byte("value1")},
			index:  0,
		}

		keyDeser := func(data []byte) (string, error) { return string(data), nil }
		valueDeser := func(data []byte) (string, error) {
			return "", errors.New("value deserialization failed")
		}

		iter := NewTypedIterator(mock, keyDeser, valueDeser)

		assert.False(t, iter.Next())
		assert.Error(t, iter.Err())
		assert.Contains(t, iter.Err().Error(), "value deserialization failed")
	})

	t.Run("propagates underlying iterator error", func(t *testing.T) {
		underlyingErr := errors.New("iterator error")
		mock := &mockIterator{
			keys:   [][]byte{},
			values: [][]byte{},
			index:  0,
			err:    underlyingErr,
		}

		keyDeser := func(data []byte) (string, error) { return string(data), nil }
		valueDeser := func(data []byte) (string, error) { return string(data), nil }

		iter := NewTypedIterator(mock, keyDeser, valueDeser)

		assert.False(t, iter.Next())
		assert.Error(t, iter.Err())
		assert.Equal(t, underlyingErr, iter.Err())
	})
}

func TestTypedIterator_KeyValue(t *testing.T) {
	t.Run("returns deserialized key and value", func(t *testing.T) {
		mock := &mockIterator{
			keys:   [][]byte{[]byte("test-key")},
			values: [][]byte{[]byte("test-value")},
			index:  0,
		}

		keyDeser := func(data []byte) (string, error) { return string(data), nil }
		valueDeser := func(data []byte) (string, error) { return string(data), nil }

		iter := NewTypedIterator(mock, keyDeser, valueDeser)

		assert.True(t, iter.Next())
		assert.Equal(t, "test-key", iter.Key())
		assert.Equal(t, "test-value", iter.Value())
	})

	t.Run("handles different types", func(t *testing.T) {
		mock := &mockIterator{
			keys:   [][]byte{{42}},
			values: [][]byte{{100, 0, 0, 0}}, // Simple int encoding
			index:  0,
		}

		keyDeser := func(data []byte) (int, error) {
			if len(data) == 0 {
				return 0, nil
			}
			return int(data[0]), nil
		}
		valueDeser := func(data []byte) (int32, error) {
			if len(data) < 4 {
				return 0, nil
			}
			return int32(data[0]), nil
		}

		iter := NewTypedIterator(mock, keyDeser, valueDeser)

		assert.True(t, iter.Next())
		assert.Equal(t, 42, iter.Key())
		assert.Equal(t, int32(100), iter.Value())
	})
}

func TestTypedIterator_Close(t *testing.T) {
	t.Run("closes successfully", func(t *testing.T) {
		mock := &mockIterator{
			keys:   [][]byte{[]byte("key")},
			values: [][]byte{[]byte("value")},
			index:  0,
		}

		keyDeser := func(data []byte) (string, error) { return string(data), nil }
		valueDeser := func(data []byte) (string, error) { return string(data), nil }

		iter := NewTypedIterator(mock, keyDeser, valueDeser)

		err := iter.Close()
		assert.NoError(t, err)
	})

	t.Run("propagates close error", func(t *testing.T) {
		closeErr := errors.New("close failed")
		mock := &mockIterator{
			keys:   [][]byte{},
			values: [][]byte{},
			index:  0,
			err:    closeErr,
		}

		keyDeser := func(data []byte) (string, error) { return string(data), nil }
		valueDeser := func(data []byte) (string, error) { return string(data), nil }

		iter := NewTypedIterator(mock, keyDeser, valueDeser)

		err := iter.Close()
		assert.Error(t, err)
		assert.Equal(t, closeErr, err)
	})
}

func TestTypedIterator_Integration(t *testing.T) {
	t.Run("full iteration lifecycle", func(t *testing.T) {
		mock := &mockIterator{
			keys: [][]byte{
				[]byte("key1"),
				[]byte("key2"),
				[]byte("key3"),
			},
			values: [][]byte{
				[]byte("value1"),
				[]byte("value2"),
				[]byte("value3"),
			},
			index: 0,
		}

		keyDeser := func(data []byte) (string, error) { return string(data), nil }
		valueDeser := func(data []byte) (string, error) { return string(data), nil }

		iter := NewTypedIterator(mock, keyDeser, valueDeser)
		defer iter.Close()

		results := []struct {
			key   string
			value string
		}{}

		for iter.Next() {
			results = append(results, struct {
				key   string
				value string
			}{
				key:   iter.Key(),
				value: iter.Value(),
			})
		}

		assert.NoError(t, iter.Err())
		assert.Equal(t, 3, len(results))
		assert.Equal(t, "key1", results[0].key)
		assert.Equal(t, "value1", results[0].value)
		assert.Equal(t, "key2", results[1].key)
		assert.Equal(t, "value2", results[1].value)
		assert.Equal(t, "key3", results[2].key)
		assert.Equal(t, "value3", results[2].value)
	})

	t.Run("returns false on deserialization error but continues underlying iteration", func(t *testing.T) {
		mock := &mockIterator{
			keys: [][]byte{
				[]byte("key1"),
				[]byte("key2"), // Will fail on this one
				[]byte("key3"),
			},
			values: [][]byte{
				[]byte("value1"),
				[]byte("value2"),
				[]byte("value3"),
			},
			index: 0,
		}

		callCount := 0
		keyDeser := func(data []byte) (string, error) {
			callCount++
			if callCount == 2 {
				return "", errors.New("deserialization error")
			}
			return string(data), nil
		}
		valueDeser := func(data []byte) (string, error) { return string(data), nil }

		iter := NewTypedIterator(mock, keyDeser, valueDeser)
		defer iter.Close()

		// First iteration succeeds
		assert.True(t, iter.Next())
		assert.Equal(t, "key1", iter.Key())

		// Second iteration fails on deserialization
		assert.False(t, iter.Next())
		assert.Error(t, iter.Err())

		// Third iteration continues (underlying iterator still has items)
		// but succeeds because deserialization works for key3
		assert.True(t, iter.Next())
		assert.Equal(t, "key3", iter.Key())

		// Now iterator is exhausted
		assert.False(t, iter.Next())
	})
}
