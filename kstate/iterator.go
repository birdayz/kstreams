package kstate

import (
	"iter"

	"github.com/birdayz/kstreams/kserde"
)

// MapIter transforms a byte iterator into a typed iterator.
// Uses provided deserializers to convert keys and values.
func MapIter[K, V any](
	seq iter.Seq2[[]byte, []byte],
	keyDeserializer kserde.Deserializer[K],
	valueDeserializer kserde.Deserializer[V],
) iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		for keyBytes, valueBytes := range seq {
			key, err := keyDeserializer(keyBytes)
			if err != nil {
				return
			}
			value, err := valueDeserializer(valueBytes)
			if err != nil {
				return
			}
			if !yield(key, value) {
				return
			}
		}
	}
}
