package internal

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/birdayz/streamz/sdk"
)

type TopologyBuilder struct {
	processors map[string]*TopologyProcessor
	stores     map[string]sdk.StoreBuilder

	sources map[string]*TopologyProcessor
}

func (t *TopologyBuilder) GetTopics() []string {
	var res []string
	for k := range t.sources {
		res = append(res, k)
	}
	return res
}

func (t *TopologyBuilder) CreateTask(tp TopicPartition) (*Task, error) {
	topic := tp.Topic
	src, ok := t.processors[topic]
	if !ok {
		return nil, errors.New("no source found")
	}

	// TODO state stores are not per topic, we maybe have to deal with it differently so they can be shared across topics
	var stores []sdk.Store
	for _, store := range t.stores {
		stores = append(stores, store.Build(tp.Partition))
	}

	builtProcessors := map[string]sdk.BaseProcessor{}

	neededProcessors := appendChildren(t, src)

	for _, pr := range neededProcessors {
		template := t.processors[pr]

		built := template.Builder()
		built.Init(stores...)
		builtProcessors[template.Name] = built
	}

	for k, parent := range builtProcessors {
		parentNode := t.processors[k]

		for _, childNodeName := range parentNode.ChildProcessors {
			childNode := t.processors[childNodeName]

			child, ok := builtProcessors[childNode.Name]
			if !ok {
				return nil, fmt.Errorf("processor %s not found", childNode.Name)
			}
			parentNode.AddChildFunc(parent, child)
		}
	}

	task := NewTask(tp.Topic, tp.Partition, builtProcessors[topic].(RecordProcessor), stores)
	return task, nil

}

func appendChildren(t *TopologyBuilder, p *TopologyProcessor) []string {
	var res []string
	res = append(res, p.Name)
	for _, child := range p.ChildProcessors {
		childProcessor := t.processors[child]

		res = append(res, appendChildren(t, childProcessor)...)

	}
	return res
}

func NewTopologyBuilder() *TopologyBuilder {
	return &TopologyBuilder{
		processors: map[string]*TopologyProcessor{},
		stores:     map[string]sdk.StoreBuilder{},
		sources:    map[string]*TopologyProcessor{},
	}
}

type TopologyProcessor struct {
	Name    string
	Builder func() sdk.BaseProcessor // Process0r -> "User processor"
	Type    reflect.Type

	ChildProcessors []string

	AddChildFunc func(parent any, child any) // Builds ProcessorNode. Call Next() for each in ChildProcessors
}

func MustAddSource[K, V any](t *TopologyBuilder, name string, topic string, keyDeserializer sdk.Deserializer[K], valueDeserializer sdk.Deserializer[V]) {
	must(AddSource(t, name, topic, keyDeserializer, valueDeserializer))
}

func AddSource[K, V any](t *TopologyBuilder, name string, topic string, keyDeserializer sdk.Deserializer[K], valueDeserializer sdk.Deserializer[V]) error {
	// TODO treat source as non-processor
	topoSource := &TopologyProcessor{
		Name: name,
		Builder: func() sdk.BaseProcessor {
			return &SourceNode[K, V]{KeyDeserializer: keyDeserializer, ValueDeserializer: valueDeserializer}
		},
		AddChildFunc: func(parent, child any) {
			parentNode, ok := parent.(*SourceNode[K, V])
			if !ok {
				panic("type error")
			}

			childNode, ok := child.(GenericProcessor[K, V])
			if !ok {
				panic("type error")

			}

			parentNode.AddNext(childNode)

		},
		ChildProcessors: []string{},
	}

	if _, found := t.processors[name]; found {
		return ErrNodeAlreadyExists
	}

	t.processors[name] = topoSource
	t.sources[name] = topoSource

	return nil
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func MustAddProcessor[Kin, Vin, Kout, Vout any](t *TopologyBuilder, p sdk.ProcessorBuilder[Kin, Vin, Kout, Vout]) {
	must(AddProcessor(t, p))
}

func AddProcessor[Kin, Vin, Kout, Vout any](t *TopologyBuilder, p sdk.ProcessorBuilder[Kin, Vin, Kout, Vout]) error {
	topoProcessor := &TopologyProcessor{
		Name: p.Name(),
		Builder: func() sdk.BaseProcessor {
			px := &Process0rNode[Kin, Vin, Kout, Vout]{
				processor: p.Build(),
				outputs:   map[string]GenericProcessor[Kout, Vout]{},
				ctx: &ProcessorContext[Kout, Vout]{
					outputs:      map[string]GenericProcessor[Kout, Vout]{},
					outputErrors: map[string]error{},
				},
			}
			return px
		},
		ChildProcessors: []string{},
	}

	topoProcessor.AddChildFunc = func(parent any, child any) {
		parentNode, ok := parent.(*Process0rNode[Kin, Vin, Kout, Vout])
		if !ok {
			panic("type error")
		}

		childNode, ok := child.(GenericProcessor[Kout, Vout])
		if !ok {
			panic("type error")
		}

		parentNode.outputs["childa"] = childNode
		parentNode.ctx.outputs["childa"] = childNode
	}

	if _, found := t.processors[p.Name()]; found {
		return ErrNodeAlreadyExists
	}

	t.processors[p.Name()] = topoProcessor

	return nil
}

func MustSetParent(t *TopologyBuilder, parent, child string) {
	must(SetParent(t, parent, child))
}

func SetParent(t *TopologyBuilder, parent, child string) error {
	parentNode, ok := t.processors[parent]
	if !ok {
		return ErrNodeNotFound
	}

	parentNode.ChildProcessors = append(parentNode.ChildProcessors, child)

	return nil

}

type Topology struct {
}

func Build() *Topology {
	return &Topology{}
}

var ErrNodeAlreadyExists = errors.New("node exists already")
var ErrNodeNotFound = errors.New("node not found")
var ErrInternal = errors.New("internal")
