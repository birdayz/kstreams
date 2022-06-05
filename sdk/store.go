package sdk

import (
	"context"
	"time"
)

type WindowKey[K any] struct {
	Key  K
	Time time.Time
}

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

type WindowedKeyValueStore[K, V any] interface {
	Store
	Set(K, V, time.Time) error
	Get(K, time.Time) (V, error)
	// Scan
}

type StoreBackend interface {
	Store
	Set(k, v []byte) error
	Get(k []byte) (v []byte, err error)
}

// TODO/FIXME make store name part of params
type StoreBuilder func(name string, partition int32) (Store, error)

type StoreBackendBuilder func(name string, p int32) (StoreBackend, error)
