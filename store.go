package kstreams

import (
	"context"
	"errors"
)

type Store interface {
	Init() error
	Flush(context.Context) error
	Close() error
}

var (
	ErrKeyNotFound = errors.New("store: key not found")
)

func RegisterStore(t *TopologyBuilder, storeBuilder StoreBuilder, name string) {
	t.stores[name] = &TopologyStore{
		Name:  name,
		Build: storeBuilder,
	}
}

func KVStore[K, V any](storeBuilder func(name string, p int32) (StoreBackend, error), keySerde SerDe[K], valueSerde SerDe[V]) func(name string, p int32) (Store, error) {
	return func(name string, p int32) (Store, error) {
		backend, err := storeBuilder(name, p)
		if err != nil {
			return nil, err
		}
		return NewKeyValueStore(backend, keySerde.Serializer, valueSerde.Serializer, keySerde.Deserializer, valueSerde.Deserializer), nil
	}
}

func NewKeyValueStore[K, V any](
	store StoreBackend,
	keySerializer Serializer[K],
	valueSerializer Serializer[V],
	keyDeserializer Deserializer[K],
	valueDeserializer Deserializer[V],
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
	store             StoreBackend
	keySerializer     Serializer[K]
	valueSerializer   Serializer[V]
	keyDeserializer   Deserializer[K]
	valueDeserializer Deserializer[V]
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

type StoreBackend interface {
	Store
	Set(k, v []byte) error
	Get(k []byte) (v []byte, err error)
}

// TODO/FIXME make store name part of params
type StoreBuilder func(name string, partition int32) (Store, error)

type StoreBackendBuilder func(name string, p int32) (StoreBackend, error)
