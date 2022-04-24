package sdk

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

type StoreBackend interface {
	Store
	Set(k, v []byte) error
	Get(k []byte) (v []byte, err error)
}

type StoreBuilder func(partition int32) Store
