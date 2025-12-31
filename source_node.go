package kstreams

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
)

// RawRecordProcessor processes raw kgo.Record objects
// This is used internally for source nodes
type RawRecordProcessor interface {
	Process(ctx context.Context, m *kgo.Record) error
}

// SourceNode[K,V] receives kgo records, and forward these to all downstream
// processors.
type SourceNode[K any, V any] struct {
	KeyDeserializer   Deserializer[K]
	ValueDeserializer Deserializer[V]

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
