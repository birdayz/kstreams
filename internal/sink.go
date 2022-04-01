package internal

import (
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

func (s *SinkNode[K, V]) Process(k K, v V) error {
	key, err := s.KeySerializer(k)
	if err != nil {
		return fmt.Errorf("sinkNode: failed to marshal key: %w", err)
	}

	value, err := s.ValueSerializer(v)
	if err != nil {
		return fmt.Errorf("sinkNode: failed to marshal value: %w", err)
	}

	// TODO

	_ = key
	_ = value

	return nil
}
