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

	// Key = TopicNode
	sources map[string]*TopologyProcessor

	processorToParent map[string]string

	processorToStores map[string][]string

	childNodes map[string][]string
}

// PartitionGroup is a set of processor names
type PartitionGroup struct {
	sourceTopics   []string
	processorNames []string
	storeNames     []string
}

func (t *TopologyBuilder) findCopartitionGroups() []*PartitionGroup {

	var partitionGroups []*PartitionGroup

	// 1. Find per partition all sub-nodes (=do DFS)
	for topic := range t.sources { // TODO: make this deterministic, ordering of keys is random

		processors := t.findAllProcessors(topic)

		// 2. Check if a state store is connected to one of these nodes.
		var storeNames []string
		for _, child := range processors {
			if stores, ok := t.processorToStores[child]; ok {
				for _, store := range stores {
					storeNames = append(storeNames, store)
				}
			}
		}

		pg := PartitionGroup{
			sourceTopics:   []string{topic},
			processorNames: processors,
			storeNames:     storeNames,
		}

		// Build new PG

		// Check if there are overlaps with other PGs. Merge PGs as needed

		// fmt.Println("Got conn stores", topic, connectedStores)

		// ?? check if any processor is already in other PG => Join other pg. What if multiple ? Join two

		// 2.1 Check if connected state stores are already in another partition group

		// 	var pgFound *PartitionGroup
		//
		// outer:
		// 	for _, store := range storeNames {
		// 		for _, pg := range partitionGroups {
		// 			if slices.Contains(pg.sourceTopics, store) {
		// 				pgFound = pg
		// 				break outer
		// 				// PG matches
		// 			}
		// 		}
		// 	}
		//
		// 	// Stores that are in PG, stores that are not in any PG ("stores we need to take care of"/move into PG)
		//
		// 	if pgFound != nil {
		// 		// 2.1a true => add this store AND these processors to "other" PG (partition group)
		// 		pgFound.sourceTopics = append(pgFound.sourceTopics, topic)
		// 		pgFound.processorNames = append(pgFound.processorNames, processors...)
		// 	} else {
		// 		// 2.1b false => Create new PG and add this stuff + state store to it
		// 	}

		partitionGroups = append(partitionGroups, &pg)

	}

	return partitionGroups
}

func (t *TopologyBuilder) findAllProcessors(processor string) []string {
	res := []string{processor}
	if children, ok := t.childNodes[processor]; ok {
		for _, child := range children {
			res = append(res, t.findAllProcessors(child)...)
		}
	}
	return res
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

	// FIXME TODO state stores are not per topic, we maybe have to deal with it differently so they can be shared across topics

	var stores []sdk.Store
	for _, store := range t.stores {
		stores = append(stores, store(tp.Partition))
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

	ps := make(map[string]RecordProcessor)
	ps[topic] = builtProcessors[topic].(RecordProcessor)

	task := NewTask(tp.Topic, tp.Partition, ps, stores)
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
		processors:        map[string]*TopologyProcessor{},
		stores:            map[string]sdk.StoreBuilder{},
		sources:           map[string]*TopologyProcessor{},
		processorToParent: map[string]string{},
		processorToStores: map[string][]string{},
		childNodes:        map[string][]string{},
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

func MustAddProcessor[Kin, Vin, Kout, Vout any](t *TopologyBuilder, p sdk.ProcessorBuilder[Kin, Vin, Kout, Vout], name string, stores ...string) {
	must(AddProcessor(t, p, name, stores...))
}

func AddProcessor[Kin, Vin, Kout, Vout any](t *TopologyBuilder, p sdk.ProcessorBuilder[Kin, Vin, Kout, Vout], name string, stores ...string) error {
	topoProcessor := &TopologyProcessor{
		Name: name,
		Builder: func() sdk.BaseProcessor {
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

	for _, store := range stores {
		if _, ok := t.stores[store]; !ok {
			return errors.New("store not found")
		}
	}

	t.processorToStores[name] = stores

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

	t.processorToParent[child] = parent

	_, ok = t.childNodes[parent]
	if !ok {
		t.childNodes[parent] = []string{}
	}

	t.childNodes[parent] = append(t.childNodes[parent], child)

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
