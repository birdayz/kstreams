package kstreams

import (
	"context"
	"errors"
	"time"

	"github.com/birdayz/kstreams/sdk"
	"github.com/birdayz/kstreams/serdes"
)

type Store interface {
	Init() error
	Flush(context.Context) error
	Close() error
}

var (
	ErrKeyNotFound = errors.New("store: key not found")
)

func RegisterStore(t *TopologyBuilder, storeBuilder sdk.StoreBuilder, name string) {
	t.stores[name] = &TopologyStore{
		Name:  name,
		Build: storeBuilder,
	}
}

func KVStore[K, V any](storeBuilder func(name string, p int32) (sdk.StoreBackend, error), keySerde sdk.SerDe[K], valueSerde sdk.SerDe[V]) func(name string, p int32) (sdk.Store, error) {
	return func(name string, p int32) (sdk.Store, error) {
		backend, err := storeBuilder(name, p)
		if err != nil {
			return nil, err
		}
		return NewKeyValueStore(backend, keySerde.Serializer, valueSerde.Serializer, keySerde.Deserializer, valueSerde.Deserializer), nil
	}
}

func WindowedStore[K, V any](storeBuilder func(name string, p int32) (sdk.StoreBackend, error), keySerde sdk.SerDe[K], valueSerde sdk.SerDe[V]) func(name string, p int32) (sdk.Store, error) {
	return func(name string, p int32) (sdk.Store, error) {
		backend, err := storeBuilder(name, p)
		if err != nil {
			return nil, err
		}

		return NewWindowedKeyValueStore(backend, keySerde.Serializer, valueSerde.Serializer, keySerde.Deserializer, valueSerde.Deserializer), nil

	}
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

type WindowedKeyValueStore[K, V any] struct {
	store                 sdk.StoreBackend
	windowKeySerializer   sdk.Serializer[sdk.WindowKey[K]]
	valueSerializer       sdk.Serializer[V]
	windowKeyDeserializer sdk.Deserializer[sdk.WindowKey[K]]
	valueDeserializer     sdk.Deserializer[V]
}

func NewWindowedKeyValueStore[K, V any](
	store sdk.StoreBackend,
	keySerializer sdk.Serializer[K],
	valueSerializer sdk.Serializer[V],
	windowKeyDeserializer sdk.Deserializer[K],
	valueDeserializer sdk.Deserializer[V],
) *WindowedKeyValueStore[K, V] {
	return &WindowedKeyValueStore[K, V]{
		store:                 store,
		windowKeySerializer:   serdes.WindowKeySerializer(keySerializer),
		valueSerializer:       valueSerializer,
		windowKeyDeserializer: serdes.WindowKeyDeserializer(windowKeyDeserializer),
		valueDeserializer:     valueDeserializer,
	}
}

func (s *WindowedKeyValueStore[K, V]) Set(k K, v V, t time.Time) error {
	wk := sdk.WindowKey[K]{
		Key:  k,
		Time: t,
	}
	keyBytes, err := s.windowKeySerializer(wk)
	if err != nil {
		return err
	}

	valueBytes, err := s.valueSerializer(v)
	if err != nil {
		return err
	}

	return s.store.Set(keyBytes, valueBytes)
}

func (s *WindowedKeyValueStore[K, V]) Get(k K, t time.Time) (V, error) {
	var v V

	wk := sdk.WindowKey[K]{
		Key:  k,
		Time: t,
	}

	key, err := s.windowKeySerializer(wk)
	if err != nil {
		return v, err
	}

	res, err := s.store.Get(key)
	if err != nil {
		return v, err
	}

	return s.valueDeserializer(res)
}

func (t *WindowedKeyValueStore[K, V]) Init() error {
	return t.store.Init()
}

func (t *WindowedKeyValueStore[K, V]) Flush(ctx context.Context) error {
	return t.store.Flush(ctx)
}

func (t *WindowedKeyValueStore[K, V]) Close() error {
	return t.store.Close()
}
