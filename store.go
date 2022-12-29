package kstreams

import (
	"context"
	"errors"

	"github.com/birdayz/kstreams/internal"
	"github.com/birdayz/kstreams/sdk"
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
		return internal.NewKeyValueStore(backend, keySerde.Serializer, valueSerde.Serializer, keySerde.Deserializer, valueSerde.Deserializer), nil
	}
}

func WindowedStore[K, V any](storeBuilder func(name string, p int32) (sdk.StoreBackend, error), keySerde sdk.SerDe[K], valueSerde sdk.SerDe[V]) func(name string, p int32) (sdk.Store, error) {
	return func(name string, p int32) (sdk.Store, error) {
		backend, err := storeBuilder(name, p)
		if err != nil {
			return nil, err
		}

		return internal.NewWindowedKeyValueStore(backend, keySerde.Serializer, valueSerde.Serializer, keySerde.Deserializer, valueSerde.Deserializer), nil

	}
}
