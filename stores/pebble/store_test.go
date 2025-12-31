package pebble

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/alecthomas/assert/v2"
	"github.com/birdayz/kstreams"
)

func TestPebbleStore(t *testing.T) {
	t.Run("basic CRUD operations", func(t *testing.T) {
		dir := t.TempDir()
		store, err := newStore(dir, "test-store", 0)
		assert.NoError(t, err)
		defer store.Close()

		// Test Set
		err = store.Set([]byte("key1"), []byte("value1"))
		assert.NoError(t, err)

		// Test Get
		value, err := store.Get([]byte("key1"))
		assert.NoError(t, err)
		assert.Equal(t, []byte("value1"), value)

		// Test Update
		err = store.Set([]byte("key1"), []byte("value1-updated"))
		assert.NoError(t, err)

		value, err = store.Get([]byte("key1"))
		assert.NoError(t, err)
		assert.Equal(t, []byte("value1-updated"), value)

		// Test Delete
		err = store.Delete([]byte("key1"))
		assert.NoError(t, err)

		// Get deleted key should return error
		_, err = store.Get([]byte("key1"))
		assert.Error(t, err)
		assert.Equal(t, kstreams.ErrKeyNotFound, err)
	})

	t.Run("get non-existent key", func(t *testing.T) {
		dir := t.TempDir()
		store, err := newStore(dir, "test-store", 0)
		assert.NoError(t, err)
		defer store.Close()

		_, err = store.Get([]byte("non-existent"))
		assert.Error(t, err)
		assert.Equal(t, kstreams.ErrKeyNotFound, err)
	})

	t.Run("nil value as tombstone", func(t *testing.T) {
		dir := t.TempDir()
		store, err := newStore(dir, "test-store", 0)
		assert.NoError(t, err)
		defer store.Close()

		// Set a value
		err = store.Set([]byte("key1"), []byte("value1"))
		assert.NoError(t, err)

		// Set nil should delete the key
		err = store.Set([]byte("key1"), nil)
		assert.NoError(t, err)

		// Key should not exist
		_, err = store.Get([]byte("key1"))
		assert.Error(t, err)
		assert.Equal(t, kstreams.ErrKeyNotFound, err)
	})

	t.Run("empty key and value", func(t *testing.T) {
		dir := t.TempDir()
		store, err := newStore(dir, "test-store", 0)
		assert.NoError(t, err)
		defer store.Close()

		// Empty key
		err = store.Set([]byte(""), []byte("value"))
		assert.NoError(t, err)

		value, err := store.Get([]byte(""))
		assert.NoError(t, err)
		assert.Equal(t, []byte("value"), value)

		// Empty value
		err = store.Set([]byte("key"), []byte(""))
		assert.NoError(t, err)

		value, err = store.Get([]byte("key"))
		assert.NoError(t, err)
		assert.Equal(t, []byte(""), value)
	})
}

func TestPebbleStoreRange(t *testing.T) {
	t.Run("forward range iteration", func(t *testing.T) {
		dir := t.TempDir()
		store, err := newStore(dir, "test-store", 0)
		assert.NoError(t, err)
		defer store.Close()

		// Populate store
		testData := map[string]string{
			"key1": "value1",
			"key2": "value2",
			"key3": "value3",
			"key4": "value4",
			"key5": "value5",
		}

		for k, v := range testData {
			err := store.Set([]byte(k), []byte(v))
			assert.NoError(t, err)
		}

		// Range from key2 to key4 (exclusive end)
		iter, err := store.Range([]byte("key2"), []byte("key4"))
		assert.NoError(t, err)
		defer iter.Close()

		results := make(map[string]string)
		for iter.Next() {
			key := string(iter.Key())
			value := string(iter.Value())
			results[key] = value
		}
		assert.NoError(t, iter.Err())

		// Should get key2 and key3
		assert.Equal(t, 2, len(results))
		assert.Equal(t, "value2", results["key2"])
		assert.Equal(t, "value3", results["key3"])
	})

	t.Run("all keys iteration", func(t *testing.T) {
		dir := t.TempDir()
		store, err := newStore(dir, "test-store", 0)
		assert.NoError(t, err)
		defer store.Close()

		// Populate store
		for i := 0; i < 10; i++ {
			key := []byte{byte(i)}
			value := []byte{byte(i * 10)}
			err := store.Set(key, value)
			assert.NoError(t, err)
		}

		// Get all keys
		iter, err := store.All()
		assert.NoError(t, err)
		defer iter.Close()

		count := 0
		for iter.Next() {
			count++
		}
		assert.NoError(t, iter.Err())
		assert.Equal(t, 10, count)
	})

	t.Run("reverse range iteration", func(t *testing.T) {
		dir := t.TempDir()
		store, err := newStore(dir, "test-store", 0)
		assert.NoError(t, err)
		defer store.Close()

		// Populate in order
		keys := []string{"a", "b", "c", "d", "e"}
		for _, k := range keys {
			err := store.Set([]byte(k), []byte("value-"+k))
			assert.NoError(t, err)
		}

		// Reverse range from b to e
		iter, err := store.ReverseRange([]byte("b"), []byte("e"))
		assert.NoError(t, err)
		defer iter.Close()

		var results []string
		for iter.Next() {
			results = append(results, string(iter.Key()))
		}
		assert.NoError(t, iter.Err())

		// Should get d, c, b in that order (reverse)
		assert.Equal(t, 3, len(results))
		assert.Equal(t, "d", results[0])
		assert.Equal(t, "c", results[1])
		assert.Equal(t, "b", results[2])
	})

	t.Run("reverse all iteration", func(t *testing.T) {
		dir := t.TempDir()
		store, err := newStore(dir, "test-store", 0)
		assert.NoError(t, err)
		defer store.Close()

		// Populate in order
		keys := []string{"a", "b", "c"}
		for _, k := range keys {
			err := store.Set([]byte(k), []byte("value"))
			assert.NoError(t, err)
		}

		// Reverse all
		iter, err := store.ReverseAll()
		assert.NoError(t, err)
		defer iter.Close()

		var results []string
		for iter.Next() {
			results = append(results, string(iter.Key()))
		}
		assert.NoError(t, iter.Err())

		// Should get c, b, a
		assert.Equal(t, 3, len(results))
		assert.Equal(t, "c", results[0])
		assert.Equal(t, "b", results[1])
		assert.Equal(t, "a", results[2])
	})

	t.Run("empty range", func(t *testing.T) {
		dir := t.TempDir()
		store, err := newStore(dir, "test-store", 0)
		assert.NoError(t, err)
		defer store.Close()

		// Range with no matching keys
		iter, err := store.Range([]byte("x"), []byte("z"))
		assert.NoError(t, err)
		defer iter.Close()

		count := 0
		for iter.Next() {
			count++
		}
		assert.NoError(t, iter.Err())
		assert.Equal(t, 0, count)
	})
}

func TestPebbleStoreIterator(t *testing.T) {
	t.Run("iterator data copy safety", func(t *testing.T) {
		dir := t.TempDir()
		store, err := newStore(dir, "test-store", 0)
		assert.NoError(t, err)
		defer store.Close()

		// Set values
		store.Set([]byte("key1"), []byte("value1"))
		store.Set([]byte("key2"), []byte("value2"))

		iter, err := store.All()
		assert.NoError(t, err)
		defer iter.Close()

		// Collect all keys and values
		var keys [][]byte
		var values [][]byte
		for iter.Next() {
			keys = append(keys, iter.Key())
			values = append(values, iter.Value())
		}

		// Verify we got data
		assert.Equal(t, 2, len(keys))
		assert.Equal(t, 2, len(values))

		// The iterator makes copies, so the data should be safe
		// even after iterator closes
		iter.Close()

		// Data should still be valid
		assert.NotZero(t, keys[0])
		assert.NotZero(t, values[0])
	})

	t.Run("iterator error handling", func(t *testing.T) {
		dir := t.TempDir()
		store, err := newStore(dir, "test-store", 0)
		assert.NoError(t, err)

		// Populate store
		store.Set([]byte("key"), []byte("value"))

		iter, err := store.All()
		assert.NoError(t, err)

		// Close store before using iterator
		store.Close()

		// Iterator should handle closed store gracefully
		// (might return false or error depending on implementation)
		for iter.Next() {
			// Try to get data
			_ = iter.Key()
			_ = iter.Value()
		}

		// Close iterator
		iter.Close()
	})

	t.Run("multiple iterators", func(t *testing.T) {
		dir := t.TempDir()
		store, err := newStore(dir, "test-store", 0)
		assert.NoError(t, err)
		defer store.Close()

		// Populate
		for i := 0; i < 5; i++ {
			key := []byte{byte(i)}
			value := []byte{byte(i * 10)}
			store.Set(key, value)
		}

		// Create multiple iterators
		iter1, err := store.All()
		assert.NoError(t, err)
		defer iter1.Close()

		iter2, err := store.Range([]byte{0}, []byte{3})
		assert.NoError(t, err)
		defer iter2.Close()

		// Both should work independently
		count1 := 0
		for iter1.Next() {
			count1++
		}

		count2 := 0
		for iter2.Next() {
			count2++
		}

		assert.Equal(t, 5, count1)
		assert.Equal(t, 3, count2)
	})
}

func TestPebbleStoreLifecycle(t *testing.T) {
	t.Run("init flush close", func(t *testing.T) {
		dir := t.TempDir()
		store, err := newStore(dir, "test-store", 0)
		assert.NoError(t, err)

		// Init
		err = store.Init()
		assert.NoError(t, err)

		// Add data
		err = store.Set([]byte("key"), []byte("value"))
		assert.NoError(t, err)

		// Flush
		err = store.Flush()
		assert.NoError(t, err)

		// Close
		err = store.Close()
		assert.NoError(t, err)
	})

	t.Run("persistence across reopens", func(t *testing.T) {
		dir := t.TempDir()

		// First session: write data
		{
			store, err := newStore(dir, "test-store", 0)
			assert.NoError(t, err)

			err = store.Set([]byte("persistent-key"), []byte("persistent-value"))
			assert.NoError(t, err)

			err = store.Flush()
			assert.NoError(t, err)

			err = store.Close()
			assert.NoError(t, err)
		}

		// Second session: verify data persists
		{
			store, err := newStore(dir, "test-store", 0)
			assert.NoError(t, err)
			defer store.Close()

			value, err := store.Get([]byte("persistent-key"))
			assert.NoError(t, err)
			assert.Equal(t, []byte("persistent-value"), value)
		}
	})

	t.Run("checkpoint is no-op", func(t *testing.T) {
		dir := t.TempDir()
		storeBackend, err := newStore(dir, "test-store", 0)
		assert.NoError(t, err)
		defer storeBackend.Close()

		// Cast to Store interface to access Checkpoint
		store := storeBackend.(*pebbleStore)

		// Checkpoint should succeed but do nothing
		err = store.Checkpoint(context.Background(), "checkpoint-1")
		assert.NoError(t, err)
	})
}

func TestPebbleStorePartitions(t *testing.T) {
	t.Run("separate stores per partition", func(t *testing.T) {
		dir := t.TempDir()

		// Create stores for different partitions
		store0, err := newStore(dir, "test-store", 0)
		assert.NoError(t, err)
		defer store0.Close()

		store1, err := newStore(dir, "test-store", 1)
		assert.NoError(t, err)
		defer store1.Close()

		// Write different data to each partition
		err = store0.Set([]byte("key"), []byte("value-partition-0"))
		assert.NoError(t, err)

		err = store1.Set([]byte("key"), []byte("value-partition-1"))
		assert.NoError(t, err)

		// Verify data is isolated per partition
		value0, err := store0.Get([]byte("key"))
		assert.NoError(t, err)
		assert.Equal(t, []byte("value-partition-0"), value0)

		value1, err := store1.Get([]byte("key"))
		assert.NoError(t, err)
		assert.Equal(t, []byte("value-partition-1"), value1)
	})

	t.Run("partition directories created", func(t *testing.T) {
		dir := t.TempDir()

		store0, err := newStore(dir, "my-store", 0)
		assert.NoError(t, err)
		defer store0.Close()

		store1, err := newStore(dir, "my-store", 1)
		assert.NoError(t, err)
		defer store1.Close()

		// Verify directories exist
		_, err = os.Stat(filepath.Join(dir, "my-store", "partition-0"))
		assert.NoError(t, err)

		_, err = os.Stat(filepath.Join(dir, "my-store", "partition-1"))
		assert.NoError(t, err)
	})
}

func TestPebbleStoreLargeData(t *testing.T) {
	t.Run("large values", func(t *testing.T) {
		dir := t.TempDir()
		store, err := newStore(dir, "test-store", 0)
		assert.NoError(t, err)
		defer store.Close()

		// Create 1MB value
		largeValue := make([]byte, 1024*1024)
		for i := range largeValue {
			largeValue[i] = byte(i % 256)
		}

		err = store.Set([]byte("large-key"), largeValue)
		assert.NoError(t, err)

		retrieved, err := store.Get([]byte("large-key"))
		assert.NoError(t, err)
		assert.Equal(t, len(largeValue), len(retrieved))
		assert.Equal(t, largeValue, retrieved)
	})

	t.Run("many keys", func(t *testing.T) {
		dir := t.TempDir()
		store, err := newStore(dir, "test-store", 0)
		assert.NoError(t, err)
		defer store.Close()

		// Write 1000 keys
		numKeys := 1000
		for i := 0; i < numKeys; i++ {
			key := []byte{byte(i >> 8), byte(i & 0xFF)}
			value := []byte{byte(i)}
			err := store.Set(key, value)
			assert.NoError(t, err)
		}

		// Verify all keys exist
		iter, err := store.All()
		assert.NoError(t, err)
		defer iter.Close()

		count := 0
		for iter.Next() {
			count++
		}
		assert.NoError(t, iter.Err())
		assert.Equal(t, numKeys, count)
	})
}

func TestNewStoreBackend(t *testing.T) {
	t.Run("creates store backend builder", func(t *testing.T) {
		dir := t.TempDir()

		builder := NewStoreBackend(dir)
		assert.NotZero(t, builder)

		// Create a store using the builder
		store, err := builder("test-store", 0)
		assert.NoError(t, err)
		assert.NotZero(t, store)

		defer store.Close()

		// Verify it works
		err = store.Set([]byte("key"), []byte("value"))
		assert.NoError(t, err)

		value, err := store.Get([]byte("key"))
		assert.NoError(t, err)
		assert.Equal(t, []byte("value"), value)
	})

	t.Run("empty state dir uses default", func(t *testing.T) {
		builder := NewStoreBackend("")

		// Should use default /tmp/kstreams
		store, err := builder("test-store", 0)
		assert.NoError(t, err)
		defer store.Close()

		// Verify it works
		err = store.Set([]byte("key"), []byte("value"))
		assert.NoError(t, err)
	})

	t.Run("multiple partitions from same builder", func(t *testing.T) {
		dir := t.TempDir()
		builder := NewStoreBackend(dir)

		// Create multiple partitions
		store0, err := builder("shared-store", 0)
		assert.NoError(t, err)
		defer store0.Close()

		store1, err := builder("shared-store", 1)
		assert.NoError(t, err)
		defer store1.Close()

		// Each should be independent
		store0.Set([]byte("key"), []byte("value0"))
		store1.Set([]byte("key"), []byte("value1"))

		val0, _ := store0.Get([]byte("key"))
		val1, _ := store1.Get([]byte("key"))

		assert.Equal(t, []byte("value0"), val0)
		assert.Equal(t, []byte("value1"), val1)
	})
}

func TestPebbleStoreEdgeCases(t *testing.T) {
	t.Run("binary keys", func(t *testing.T) {
		dir := t.TempDir()
		store, err := newStore(dir, "test-store", 0)
		assert.NoError(t, err)
		defer store.Close()

		// Keys with null bytes and other binary data
		binaryKey := []byte{0x00, 0xFF, 0x00, 0xAA, 0x55}
		value := []byte("binary-key-value")

		err = store.Set(binaryKey, value)
		assert.NoError(t, err)

		retrieved, err := store.Get(binaryKey)
		assert.NoError(t, err)
		assert.Equal(t, value, retrieved)
	})

	t.Run("overwrite operations", func(t *testing.T) {
		dir := t.TempDir()
		store, err := newStore(dir, "test-store", 0)
		assert.NoError(t, err)
		defer store.Close()

		key := []byte("key")

		// Write, overwrite multiple times
		for i := 0; i < 100; i++ {
			value := []byte{byte(i)}
			err := store.Set(key, value)
			assert.NoError(t, err)
		}

		// Should have final value
		value, err := store.Get(key)
		assert.NoError(t, err)
		assert.Equal(t, []byte{99}, value)
	})

	t.Run("delete non-existent key", func(t *testing.T) {
		dir := t.TempDir()
		store, err := newStore(dir, "test-store", 0)
		assert.NoError(t, err)
		defer store.Close()

		// Deleting non-existent key should not error
		err = store.Delete([]byte("non-existent"))
		assert.NoError(t, err)
	})
}
