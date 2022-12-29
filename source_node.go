package kstreams

import (
	"context"

	"github.com/birdayz/kstreams/sdk"
	"github.com/twmb/franz-go/pkg/kgo"
)

type RecordProcessor interface {
	Process(ctx context.Context, m *kgo.Record) error
}

// SourceNode[K,V] receives kgo records, and forward these to all downstream
// processors.
type SourceNode[K any, V any] struct {
	KeyDeserializer   sdk.Deserializer[K]
	ValueDeserializer sdk.Deserializer[V]

	DownstreamProcessors []InputProcessor[K, V]
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

	for _, next := range n.DownstreamProcessors {
		if err := next.Process(ctx, key, value); err != nil {
			return err
		}
	}

	return nil
}

func (n *SourceNode[K, V]) AddNext(next InputProcessor[K, V]) {
	n.DownstreamProcessors = append(n.DownstreamProcessors, next)
}
