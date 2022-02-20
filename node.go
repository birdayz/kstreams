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

type Nexter[K, V any] interface {
	AddNext(GenericProcessor[K, V])
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

func (n *SourceNode[K, V]) AddNext(next GenericProcessor[K, V]) {
	n.Next = next
}

type ProcessorNode[K1, V1, K2, V2 any] struct {
	UserFunc ProcessFunc[K1, V1, K2, V2]

	Next GenericProcessor[K2, V2]
}

func (n *ProcessorNode[K, V, K2, V2]) AddNext(next GenericProcessor[K2, V2]) {
	n.Next = next
}

func (p *ProcessorNode[K1, V1, K2, V2]) Process(key K1, value V1) error {
	k2, v2, err := p.UserFunc(key, value)
	if err != nil {
		return err
	}

	if p.Next != nil {
		return p.Next.Process(k2, v2) // TODO: Handle multiple!
	}
	return nil
}

type SinkNode[K any, V any] struct {
	KeySerializer   Serializer[K]
	ValueSerializer Serializer[V]

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

	_ = key
	_ = value

	return nil
}

type Process0r[Kin any, Vin any, Kout any, Vout any] interface {
	Process(ctx Context[Kout, Vout], k Kin, v Vin) error
}

type Context[Kout any, Vout any] interface {
	Forward(k Kout, v Vout)
}

type Process0rNode[Kin any, Vin any, Kout any, Vout any] struct {
	processor Process0r[Kin, Vin, Kout, Vout]
	outputs   map[string]GenericProcessor[Kout, Vout]

	ctx *ProcessorContext[Kout, Vout]
}

func (p *Process0rNode[Kin, Vin, Kout, Vout]) Process(k Kin, v Vin) error {
	err := p.processor.Process(p.ctx, k, v)
	if err != nil {
		return err
	}

	var firstError error
	for _, err := range p.ctx.outputErrors {
		firstError = err
	}
	if firstError != nil {
		p.ctx.outputErrors = map[string]error{}
	}

	return firstError
}

type ProcessorContext[Kout any, Vout any] struct {
	outputs map[string]GenericProcessor[Kout, Vout]

	outputErrors map[string]error
}

func (c *ProcessorContext[Kout, Vout]) Forward(k Kout, v Vout) {
	for name, p := range c.outputs {
		if err := p.Process(k, v); err != nil {
			c.outputErrors[name] = err
		}
	}
}
