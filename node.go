package streamz

import (
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

type RecordProcessor interface {
	Process(m *kgo.Record) error
}

type GenericProcessor[K any, V any] interface {
	Process(K, V) error
}

type SourceNode[K any, V any] struct {
	KeyDeserializer   Deserializer[K]
	ValueDeserializer Deserializer[V]

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

type ProcessorNode[K1, V1, K2, V2 any] struct {
	UserFunc ProcessFunc[K1, V1, K2, V2]

	Next GenericProcessor[K2, V2]
}

func (p *ProcessorNode[K1, V1, K2, V2]) Process(key K1, value V1) error {
	k2, v2, err := p.UserFunc(key, value)
	if err != nil {
		return err
	}

	return p.Next.Process(k2, v2) // TODO: Handle multiple!
}

type SinkNode[K any, V any] struct {
	KeySerializer   Serializer[K]
	ValueSerializer Serializer[V]

	client *kgo.Client
}

func (s *SinkNode[K, V]) Process(k K, v V) error {
	fmt.Println("Reached sink", k, v)
	return nil
}
