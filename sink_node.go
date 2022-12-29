package kstreams

import (
	"context"
	"fmt"
	"sync"

	"github.com/birdayz/kstreams/sdk"
	"github.com/twmb/franz-go/pkg/kgo"
)

var _ = InputProcessor[any, any](&SinkNode[any, any]{})

type SinkNode[K any, V any] struct {
	KeySerializer   sdk.Serializer[K]
	ValueSerializer sdk.Serializer[V]

	client *kgo.Client
	topic  string

	futuresWg sync.WaitGroup
	futures   []produceResult

	// TODO maybe configure max inflight
}

type produceResult struct {
	record *kgo.Record
	err    error
}

func NewSinkNode[K, V any](client *kgo.Client, topic string, keySerializer sdk.Serializer[K], valueSerializer sdk.Serializer[V]) *SinkNode[K, V] {
	return &SinkNode[K, V]{
		client:          client,
		topic:           topic,
		KeySerializer:   keySerializer,
		ValueSerializer: valueSerializer,
		futures:         []produceResult{},
	}
}

func (s *SinkNode[K, V]) Process(ctx context.Context, k K, v V) error {
	key, err := s.KeySerializer(k)
	if err != nil {
		return fmt.Errorf("sinkNode: failed to marshal key: %w", err)
	}

	value, err := s.ValueSerializer(v)
	if err != nil {
		return fmt.Errorf("sinkNode: failed to marshal value: %w", err)
	}

	s.futuresWg.Add(1)
	s.client.Produce(context.Background(), &kgo.Record{
		Key:   key,
		Value: value,
		Topic: s.topic,
	}, func(r *kgo.Record, err error) {

		pr := produceResult{
			record: r,
			err:    err,
		}
		s.futures = append(s.futures, pr)
		s.futuresWg.Done()

	})

	return nil
}

func (s *SinkNode[K, V]) Flush(ctx context.Context) error {
	s.futuresWg.Wait()

	for _, result := range s.futures {
		if err := result.err; err != nil {
			return fmt.Errorf("failed to produce: %w", result.err)
		}
	}

	// Keep allocated memory.
	s.futures = s.futures[:0]

	return nil
}

type Flusher interface {
	Flush(context.Context) error
}
