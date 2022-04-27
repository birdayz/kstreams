package internal

import (
	"context"
	"fmt"

	"github.com/birdayz/streamz/sdk"
	"github.com/twmb/franz-go/pkg/kgo"
)

type SinkNode[K any, V any] struct {
	KeySerializer   sdk.Serializer[K]
	ValueSerializer sdk.Serializer[V]

	client *kgo.Client
	topic  string
}

func NewSinkNode[K, V any](client *kgo.Client, topic string, keySerializer sdk.Serializer[K], valueSerializer sdk.Serializer[V]) *SinkNode[K, V] {
	return &SinkNode[K, V]{
		client:          client,
		topic:           topic,
		KeySerializer:   keySerializer,
		ValueSerializer: valueSerializer,
	}
}

func (s *SinkNode[K, V]) Process(k K, v V) error {
	key, err := s.KeySerializer(k)
	if err != nil {
		return fmt.Errorf("sinkNode: failed to marshal key: %w", err)
	}

	value, err := s.ValueSerializer(v)
	if err != nil {
		return fmt.Errorf("sinkNode: failed to marshal value: %w", err)
	}

	s.client.Produce(context.Background(), &kgo.Record{
		Key:   key,
		Value: value,
		Topic: s.topic,
	}, nil)

	return nil
}
