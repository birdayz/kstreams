package sdk

import "context"

type Store interface {
	Init() error
	Flush(context.Context) error
	Close() error
}

type KeyValueStore[K, V any] interface {
	Store
	Set(K, V) error
	Get(K) (V, error)
}

type StoreBackend interface {
	Store
	Set(k, v []byte) error
	Get(k []byte) (v []byte, err error)
}

// TODO/FIXME make store name part of params
type StoreBuilder func(partition int32) (Store, error)
