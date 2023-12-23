package kstreams

import "github.com/twmb/franz-go/pkg/kgo"

type Store interface {
	Init() error
	Flush() error
	Close() error
}

func NewChangeLoggingKeyValueStateStore[K, V any](client *kgo.Client) *ChangeLoggingKeyValueStateStore[K, V] {
	return &ChangeLoggingKeyValueStateStore[K, V]{}
}

type ChangeLoggingKeyValueStateStore[K, V any] struct {
	client *kgo.Client
}

func (s *ChangeLoggingKeyValueStateStore[K, V]) Init() error {
	return nil
}

func (s *ChangeLoggingKeyValueStateStore[K, V]) Flush() error {
	return nil
}

func (s *ChangeLoggingKeyValueStateStore[K, V]) Close() error {
	return nil
}

func (t *ChangeLoggingKeyValueStateStore[K, V]) Set(k K, v V) error {
	return nil
}

func (t *ChangeLoggingKeyValueStateStore[K, V]) Get(k K) (V, error) {
	result := new(V)
	return *result, nil
}
