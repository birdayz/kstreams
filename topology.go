package streamz

import (
	"errors"
	"fmt"
	"reflect"
)

type TopologyBuilder struct {
	processors map[string]*TopologyProcessor
}

func (t *TopologyBuilder) CreateTask(tp TopicPartition) (*Task, error) {
	fmt.Println("CR", tp)
	topic := tp.Topic
	src, ok := t.processors[topic]
	if !ok {
		return nil, errors.New("no source found")
	}

	builtProcessors := map[string]any{}

	neededProcessors := appendChildren(t, src)

	for _, pr := range neededProcessors {
		x := t.processors[pr]

		built := x.Builder()
		builtProcessors[x.Name] = built
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

	task := &Task{
		rootNode: builtProcessors[topic].(RecordProcessor),
	}
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
	}
}

type TopologyProcessor struct {
	Name    string
	Builder func() any // Process0r -> "User processor"
	Type    reflect.Type

	ChildProcessors []string

	AddChildFunc func(parent any, child any) // Builds ProcessorNode. Call Next() for each in ChildProcessors
}

func AddSource[K, V any](t *TopologyBuilder, name string, topic string, keyDeserializer Deserializer[K], valueDeserializer Deserializer[V]) error {
	topoSource := &TopologyProcessor{
		Name: name,
		Builder: func() any {
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

	return nil
}

func AddProcessor[Kin, Vin, Kout, Vout any](t *TopologyBuilder, name string, p func() Process0r[Kin, Vin, Kout, Vout]) error {
	topoProcessor := &TopologyProcessor{
		Name: name,
		Builder: func() any {
			px := &Process0rNode[Kin, Vin, Kout, Vout]{
				processor: p(),
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

	if _, found := t.processors[name]; found {
		return ErrNodeAlreadyExists
	}

	t.processors[name] = topoProcessor

	return nil
}

func SetParent[Kchild, Vchild, Kparent, Vparent any](t *TopologyBuilder, parent string, child string) error {
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
