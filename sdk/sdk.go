package sdk

import (
	"errors"
)

type Context[Kout any, Vout any] interface {
	Forward(k Kout, v Vout)
}

type Processor[Kin any, Vin any, Kout any, Vout any] interface {
	Process(ctx Context[Kout, Vout], k Kin, v Vin) error
}

type ProcessorBuilder[Kin any, Vin any, Kout any, Vout any] interface {
	Name() string
	Build() Processor[Kin, Vin, Kout, Vout]
}

type Serializer[T any] func(T) ([]byte, error)

type Deserializer[T any] func([]byte) (T, error)

type SerDe[T any] struct {
	Serializer   Serializer[T]
	Deserializer Deserializer[T]
}

type Store interface {
	Init() error
	Flush() error
	Close() error
}

type KeyValueStore[K, V any] interface {
	Store
	Set(K, V) error
	Get(K) (V, error)
}

type KeyValueByteStore interface {
	Store
	Set(k, v []byte) error
	Get(k []byte) (v []byte, err error)
}

func NewTypedStateStore[K, V any](store KeyValueByteStore, keySerializer Serializer[K], valueSerializer Serializer[V], keyDeserializer Deserializer[K], valueDeserializer Deserializer[V]) *GenericStateStore[K, V] {
	return &GenericStateStore[K, V]{
		store:             store,
		keySerializer:     keySerializer,
		valueSerializer:   valueSerializer,
		keyDeserializer:   keyDeserializer,
		valueDeserializer: valueDeserializer,
	}
}

type GenericStateStore[K, V any] struct {
	store             KeyValueByteStore
	keySerializer     Serializer[K]
	valueSerializer   Serializer[V]
	keyDeserializer   Deserializer[K]
	valueDeserializer Deserializer[V]
}

func (t *GenericStateStore[K, V]) Init() error {
	return t.store.Init()
}

func (t *GenericStateStore[K, V]) Flush() error {
	return t.store.Flush()
}

func (t *GenericStateStore[K, V]) Close() error {
	return t.store.Close()
}

func (t *GenericStateStore[K, V]) Set(k K, v V) error {
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

func (t *GenericStateStore[K, V]) Get(k K) (V, error) {
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

var ErrNotFound = errors.New("store: not found")
