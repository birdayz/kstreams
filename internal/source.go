package internal

import (
	"context"

	"github.com/birdayz/streamz/sdk"
	"github.com/twmb/franz-go/pkg/kgo"
)

type RecordProcessor interface {
	Process(ctx context.Context, m *kgo.Record) error
}

type SourceNode[K any, V any] struct {
	KeyDeserializer   sdk.Deserializer[K]
	ValueDeserializer sdk.Deserializer[V]

	Nexts []GenericProcessor[K, V]
}

func (n *SourceNode[K, V]) Process(ctx context.Context, m *kgo.Record) error {
	key, err := n.KeyDeserializer(m.Key)
	if err != nil {
		return err
	}

	value, err := n.ValueDeserializer(m.Value)
	if err != nil {
		return err
	}

	for _, next := range n.Nexts {
		if err := next.Process(ctx, key, value); err != nil {
			return err
		}
	}

	return nil
}

func (n *SourceNode[K, V]) Close() error {
	return nil
}

func (n *SourceNode[K, V]) Init(store ...sdk.Store) error {
	return nil
}

func (n *SourceNode[K, V]) AddNext(next GenericProcessor[K, V]) {
	n.Nexts = append(n.Nexts, next)
}
