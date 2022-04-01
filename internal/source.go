package internal

import (
	"github.com/birdayz/streamz/sdk"
	"github.com/twmb/franz-go/pkg/kgo"
)

type RecordProcessor interface {
	Process(m *kgo.Record) error
}

type SourceNode[K any, V any] struct {
	KeyDeserializer   sdk.Deserializer[K]
	ValueDeserializer sdk.Deserializer[V]

	Next GenericProcessor[K, V]
}

func (n *SourceNode[K, V]) Process(m *kgo.Record) error {
	key, err := n.KeyDeserializer(m.Key)
	if err != nil {
		return err
	}

	value, err := n.ValueDeserializer(m.Value)
	if err != nil {
		return err
	}

	return n.Next.Process(key, value)
}

func (n *SourceNode[K, V]) AddNext(next GenericProcessor[K, V]) {
	n.Next = next
}
