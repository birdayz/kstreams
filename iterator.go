package kstreams

// Iterator provides an interface for iterating over key-value pairs
// This is used for store range queries and full scans
type Iterator interface {
	// Next advances the iterator to the next key-value pair
	// Returns true if a pair is available, false if iteration is complete
	Next() bool

	// Key returns the current key
	// Only valid after Next() returns true
	Key() []byte

	// Value returns the current value
	// Only valid after Next() returns true
	Value() []byte

	// Err returns any error encountered during iteration
	Err() error

	// Close releases resources associated with the iterator
	Close() error
}

// TypedIterator wraps an Iterator and provides type-safe access to keys and values
type TypedIterator[K, V any] struct {
	iter              Iterator
	keyDeserializer   Deserializer[K]
	valueDeserializer Deserializer[V]

	currentKey   K
	currentValue V
	err          error
}

// NewTypedIterator creates a new TypedIterator
func NewTypedIterator[K, V any](
	iter Iterator,
	keyDeserializer Deserializer[K],
	valueDeserializer Deserializer[V],
) *TypedIterator[K, V] {
	return &TypedIterator[K, V]{
		iter:              iter,
		keyDeserializer:   keyDeserializer,
		valueDeserializer: valueDeserializer,
	}
}

// Next advances the iterator to the next key-value pair
// Returns true if a pair is available, false if iteration is complete
func (it *TypedIterator[K, V]) Next() bool {
	if !it.iter.Next() {
		return false
	}

	// Deserialize key
	key, err := it.keyDeserializer(it.iter.Key())
	if err != nil {
		it.err = err
		return false
	}
	it.currentKey = key

	// Deserialize value
	value, err := it.valueDeserializer(it.iter.Value())
	if err != nil {
		it.err = err
		return false
	}
	it.currentValue = value

	return true
}

// Key returns the current key
// Only valid after Next() returns true
func (it *TypedIterator[K, V]) Key() K {
	return it.currentKey
}

// Value returns the current value
// Only valid after Next() returns true
func (it *TypedIterator[K, V]) Value() V {
	return it.currentValue
}

// Err returns any error encountered during iteration
func (it *TypedIterator[K, V]) Err() error {
	if it.err != nil {
		return it.err
	}
	return it.iter.Err()
}

// Close releases resources associated with the iterator
func (it *TypedIterator[K, V]) Close() error {
	return it.iter.Close()
}
