package kstreams

import (
	"errors"

	"github.com/twmb/franz-go/pkg/kgo"
)

type Nexter[K, V any] interface {
	AddNext(InputProcessor[K, V])
}

type TopologyBuilder struct {
	processors map[string]*TopologyProcessor
	stores     map[string]*TopologyStore

	// Key = TopicNode
	sources map[string]*TopologySource
	sinks   map[string]*TopologySink
}

func (tb *TopologyBuilder) Build() (*Topology, error) {
	// Validate for cycles
	return &Topology{
		sources:    tb.sources,
		stores:     tb.stores,
		processors: tb.processors,
		sinks:      tb.sinks,
	}, nil
}

func (tb *TopologyBuilder) MustBuild() *Topology {
	topology, err := tb.Build()
	if err != nil {
		panic(err)
	}
	return topology
}

// Contains reports whether v is present in s.
func ContainsAny[E comparable](s []E, v []E) bool {
	for _, item := range s {
		for _, check := range v {
			if item == check {
				return true
			}
		}
	}

	return false
}

func NewTopologyBuilder() *TopologyBuilder {
	return &TopologyBuilder{
		processors: map[string]*TopologyProcessor{},
		stores:     map[string]*TopologyStore{},
		sources:    map[string]*TopologySource{},
		sinks:      map[string]*TopologySink{},
	}
}

type TopologyStore struct {
	Name  string
	Build StoreBuilder
}

type TopologySink struct {
	Name    string
	Builder func(*kgo.Client) Flusher
}

type TopologyProcessor struct {
	Name           string
	Build          func() BaseProcessor
	ChildNodeNames []string
	AddChildFunc   func(parent any, child any, childName string) // TODO - possible to do w/o parent ?
	StoreNames     []string
}

type TopologySource struct {
	Name           string
	Build          func() RecordProcessor
	ChildNodeNames []string
	AddChildFunc   func(parent any, child any, childName string) // TODO - possible to do w/o parent ?
}

func MustRegisterSource[K, V any](t *TopologyBuilder, name string, topic string, keyDeserializer Deserializer[K], valueDeserializer Deserializer[V]) {
	must(RegisterSource(t, name, topic, keyDeserializer, valueDeserializer))
}

func RegisterSource[K, V any](t *TopologyBuilder, name string, topic string, keyDeserializer Deserializer[K], valueDeserializer Deserializer[V]) error {
	topoSource := &TopologySource{
		Name: name,
		Build: func() RecordProcessor {
			return &SourceNode[K, V]{KeyDeserializer: keyDeserializer, ValueDeserializer: valueDeserializer}
		},
		AddChildFunc: func(parent, child any, childName string) {
			parentNode, ok := parent.(*SourceNode[K, V])
			if !ok {
				panic("type error")
			}

			childNode, ok := child.(InputProcessor[K, V])
			if !ok {
				panic("type error")

			}

			parentNode.AddNext(childNode)

		},
		ChildNodeNames: []string{},
	}

	if _, found := t.processors[name]; found {
		return ErrNodeAlreadyExists
	}

	t.sources[name] = topoSource

	return nil
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func MustRegisterSink[K, V any](t *TopologyBuilder, name, topic string, keySerializer Serializer[K], valueSerializer Serializer[V], parent string) {
	must(RegisterSink(t, name, topic, keySerializer, valueSerializer, parent))
}

func RegisterSink[K, V any](t *TopologyBuilder, name, topic string, keySerializer Serializer[K], valueSerializer Serializer[V], parent string) error {
	topoSink := &TopologySink{
		Name: name,
		Builder: func(client *kgo.Client) Flusher {
			return NewSinkNode(client, topic, keySerializer, valueSerializer)
		},
	}

	// t.processors[name] = topoProcessor
	t.sinks[name] = topoSink

	return SetParent(t, parent, name)
}

func MustRegisterProcessor[Kin, Vin, Kout, Vout any](t *TopologyBuilder, p ProcessorBuilder[Kin, Vin, Kout, Vout], name string, parent string, stores ...string) { // TODO: change to functional option for stores
	must(RegisterProcessor(t, p, name, parent, stores...))
}

func RegisterProcessor[Kin, Vin, Kout, Vout any](t *TopologyBuilder, p ProcessorBuilder[Kin, Vin, Kout, Vout], name string, parent string, stores ...string) error {
	topoProcessor := &TopologyProcessor{
		Name: name,
		Build: func() BaseProcessor {
			px := &ProcessorNode[Kin, Vin, Kout, Vout]{
				userProcessor: p(),
				outputs:       map[string]InputProcessor[Kout, Vout]{},
			}
			return px
		},
		ChildNodeNames: []string{},
		StoreNames:     stores,
	}

	// TODO validate store names, existence ?

	topoProcessor.AddChildFunc = func(parent any, child any, childName string) {
		// TODO: try to detect these already when building the topology.
		parentNode, ok := parent.(*ProcessorNode[Kin, Vin, Kout, Vout])
		if !ok {
			panic("type error")
		}

		// TODO: try to detect these already when building the topology.
		childNode, ok := child.(InputProcessor[Kout, Vout])
		if !ok {
			panic("type error")
		}

		parentNode.outputs[childName] = childNode
	}

	if _, found := t.processors[name]; found {
		return ErrNodeAlreadyExists
	}

	t.processors[name] = topoProcessor

	for _, store := range stores {
		if _, ok := t.stores[store]; !ok {
			return errors.New("store not found")
		}
	}
	return SetParent(t, parent, name)
}

func MustSetParent(t *TopologyBuilder, parent, child string) {
	must(SetParent(t, parent, child))
}

func SetParent(t *TopologyBuilder, parent, child string) error {
	parentNode, ok := t.processors[parent]
	if ok {
		parentNode.ChildNodeNames = append(parentNode.ChildNodeNames, child)
		return nil
	}

	source, ok := t.sources[parent]
	if ok {
		source.ChildNodeNames = append(source.ChildNodeNames, child)
		return nil
	}

	return ErrNodeNotFound
}

var ErrNodeAlreadyExists = errors.New("node exists already")
var ErrNodeNotFound = errors.New("node not found")
var ErrInternal = errors.New("internal")
