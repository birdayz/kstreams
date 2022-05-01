package streamz

import (
	"context"
	"errors"

	"github.com/birdayz/streamz/internal"
	"github.com/birdayz/streamz/sdk"
)

var (
	ErrKeyNotFound = errors.New("store: key not found")
)

func RegisterStore(t *Topology, storeBuilder sdk.StoreBuilder, name string) {
	internal.RegisterStore(t, storeBuilder, name)

}

func NewKeyValueStore[K, V any](
	store sdk.StoreBackend,
	keySerializer sdk.Serializer[K],
	valueSerializer sdk.Serializer[V],
	keyDeserializer sdk.Deserializer[K],
	valueDeserializer sdk.Deserializer[V],
) *KeyValueStore[K, V] {
	return &KeyValueStore[K, V]{
		store:             store,
		keySerializer:     keySerializer,
		valueSerializer:   valueSerializer,
		keyDeserializer:   keyDeserializer,
		valueDeserializer: valueDeserializer,
	}
}

type KeyValueStore[K, V any] struct {
	store             sdk.StoreBackend
	keySerializer     sdk.Serializer[K]
	valueSerializer   sdk.Serializer[V]
	keyDeserializer   sdk.Deserializer[K]
	valueDeserializer sdk.Deserializer[V]
}

func (t *KeyValueStore[K, V]) Init() error {
	return t.store.Init()
}

func (t *KeyValueStore[K, V]) Flush(ctx context.Context) error {
	return t.store.Flush(ctx)
}

func (t *KeyValueStore[K, V]) Close() error {
	return t.store.Close()
}

func (t *KeyValueStore[K, V]) Set(k K, v V) error {
	key, err := t.keySerializer(k)
	if err != nil {
		return err
	}

	value, err := t.valueSerializer(v)
	if err != nil {
		return err
	}

	return t.store.Set(key, value)
}

func (t *KeyValueStore[K, V]) Get(k K) (V, error) {
	var v V
	key, err := t.keySerializer(k)
	if err != nil {
		return v, err
	}

	res, err := t.store.Get(key)
	if err != nil {
		return v, err
	}

	return t.valueDeserializer(res)
}

func WrapStore[K, V any](storeBuilder func(p int32) (sdk.StoreBackend, error), keySerde sdk.SerDe[K], valueSerde sdk.SerDe[V]) func(p int32) (sdk.Store, error) {
	return func(p int32) (sdk.Store, error) {
		backend, err := storeBuilder(p)
		if err != nil {
			return nil, err
		}
		return NewKeyValueStore(backend, keySerde.Serializer, valueSerde.Serializer, keySerde.Deserializer, valueSerde.Deserializer), nil
	}
}
