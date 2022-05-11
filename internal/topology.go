package internal

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/birdayz/streamz/sdk"
	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/exp/slices"
)

type TopologyBuilder struct {
	processors map[string]*TopologyProcessor
	stores     map[string]sdk.StoreBuilder

	// Key = TopicNode
	sources map[string]*TopologyProcessor
	sinks   map[string]*TopologySink

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

func mergeIteration(pgs []*PartitionGroup) (altered []*PartitionGroup, done bool) {
	var a, b int

	var dirty bool
outer:
	for i, pg := range pgs {

		for d, otherPg := range pgs {
			if i == d {
				continue
			}
			if ContainsAny(otherPg.sourceTopics, pg.sourceTopics) || ContainsAny(otherPg.processorNames, pg.processorNames) || ContainsAny(otherPg.storeNames, pg.storeNames) {
				a = i
				b = d
				dirty = true
				break outer
			}
		}
	}

	// Clean, return
	if !dirty {
		return pgs, true
	}

	// "Sort" so it's deterministic.
	if a < b {
		a, b = b, a
	}

	// Merge b into a.
	pgA := pgs[a]
	pgB := pgs[b]

	pgA.sourceTopics = slices.Compact(append(pgA.sourceTopics, pgB.sourceTopics...))
	pgA.processorNames = slices.Compact(append(pgA.processorNames, pgB.processorNames...))
	pgA.storeNames = slices.Compact(append(pgA.storeNames, pgB.storeNames...))

	pgs = slices.Delete(pgs, b, b+1)

	return pgs, false
}

func mergePartitionGroups(pgs []*PartitionGroup) []*PartitionGroup {
	finished := false
	for !finished {
		pgs, finished = mergeIteration(pgs)
	}

	return pgs
}

func (t *TopologyBuilder) partitionGroups() []*PartitionGroup {

	var pgs []*PartitionGroup

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

		pg := &PartitionGroup{
			sourceTopics:   []string{topic},
			processorNames: processors,
			storeNames:     storeNames,
		}

		pgs = append(pgs, pg)
	}

	pgs = mergePartitionGroups(pgs)

	for _, pg := range pgs {
		slices.Sort(pg.sourceTopics)
		slices.Sort(pg.processorNames)
		slices.Sort(pg.storeNames)
	}

	return pgs
}

func (t *TopologyBuilder) findAllProcessors(processor string) []string {
	var res []string
	if children, ok := t.childNodes[processor]; ok {
		for _, child := range children {
			// Ignore sinks
			if _, ok := t.sinks[child]; ok {
				continue
			}
			res = append(res, child)
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

// CreateTask returns a Task for the given topics. It contains all topics, and
// their child processors & sinks. sdfhjsdfj asdfjl dsjlfk
func (t *TopologyBuilder) CreateTask(topics []string, partition int32, client *kgo.Client) (*Task, error) {
	var srcs []*TopologyProcessor

	for _, topic := range topics {
		src, ok := t.processors[topic]
		if !ok {
			return nil, errors.New("no source found")
		}
		srcs = append(srcs, src)
	}

	var stores []sdk.Store
	for name, store := range t.stores {
		builtStore, err := store(name, partition)
		if err != nil {
			return nil, fmt.Errorf("failed to build store: %w", err)
		}
		stores = append(stores, builtStore)
	}

	builtProcessors := map[string]sdk.BaseProcessor{}

	var neededProcessors []string
	for _, src := range srcs {
		neededProcessors = append(neededProcessors, appendChildren(t, src)...)
	}
	neededProcessors = slices.Compact(neededProcessors)

	builtSinks := map[string]Flusher{}

	for _, pr := range neededProcessors {
		topoProcessor, ok := t.processors[pr]
		if ok {
			built := topoProcessor.Builder()

			built.Init(stores...) // TODO move init into task. Topo only creates, task inits closes
			builtProcessors[topoProcessor.Name] = built
		} else {
			topoSink, ok := t.sinks[pr]
			if !ok {
				return nil, fmt.Errorf("could not find node: %s", pr)
			}

			sink := topoSink.Builder(client)
			builtSinks[topoSink.Name] = sink
		}

	}

	for k, builtProcessor := range builtProcessors {
		node := t.processors[k]

		for _, childNodeName := range node.ChildNodes {
			childNode, ok := t.processors[childNodeName]
			if ok {

				child, ok := builtProcessors[childNode.Name]
				if !ok {
					return nil, fmt.Errorf("processor %s not found", childNode.Name)
				}
				node.AddChildFunc(builtProcessor, child, childNode.Name)
			} else {
				childSink, ok := t.sinks[childNodeName]
				if !ok {
					return nil, fmt.Errorf("could not find child node %s", childNodeName)
				}

				child, ok := builtSinks[childSink.Name]
				if !ok {
					return nil, fmt.Errorf("could not find child node %s", childNodeName)
				}

				node.AddChildFunc(builtProcessor, child, childSink.Name)

			}
		}
	}

	ps := make(map[string]RecordProcessor)
	for _, topic := range topics {
		ps[topic] = builtProcessors[topic].(RecordProcessor)
	}

	task := NewTask(topics, partition, ps, stores, builtProcessors, builtSinks)
	return task, nil

}

func appendChildren(t *TopologyBuilder, p *TopologyProcessor) []string {
	var res []string
	res = append(res, p.Name)
	for _, child := range p.ChildNodes {
		childProcessor, ok := t.processors[child]
		if ok {
			res = append(res, appendChildren(t, childProcessor)...)
		} else {
			_, ok := t.sinks[child]
			if !ok {
				// TODO handle. Neither processor, not sink found.
			}

			// It's a sink. Since this is terminal, we don't need to go further and
			// can just add the name
			res = append(res, child)
		}

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
		sinks:             map[string]*TopologySink{},
	}
}

type TopologySink struct {
	Name    string
	Builder func(*kgo.Client) Flusher
}

type TopologyProcessor struct {
	Name    string
	Builder func() sdk.BaseProcessor // Process0r -> "User processor"
	Type    reflect.Type

	ChildNodes []string

	AddChildFunc func(parent any, child any, childName string) // Builds ProcessorNode. Call Next() for each in ChildProcessors
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
		AddChildFunc: func(parent, child any, childName string) {
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
		ChildNodes: []string{},
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

func MustAddSink[K, V any](t *TopologyBuilder, name, topic string, keySerializer sdk.Serializer[K], valueSerializer sdk.Serializer[V]) {
	must(AddSink(t, name, topic, keySerializer, valueSerializer))
}

func AddSink[K, V any](t *TopologyBuilder, name, topic string, keySerializer sdk.Serializer[K], valueSerializer sdk.Serializer[V]) error {
	topoSink := &TopologySink{
		Name: name,
		Builder: func(client *kgo.Client) Flusher {
			return NewSinkNode(client, topic, keySerializer, valueSerializer)
		},
	}

	// t.processors[name] = topoProcessor
	t.sinks[name] = topoSink

	return nil
}

func MustAddProcessor[Kin, Vin, Kout, Vout any](t *TopologyBuilder, p sdk.ProcessorBuilder[Kin, Vin, Kout, Vout], name string, stores ...string) {
	must(AddProcessor(t, p, name, stores...))
}

func AddProcessor[Kin, Vin, Kout, Vout any](t *TopologyBuilder, p sdk.ProcessorBuilder[Kin, Vin, Kout, Vout], name string, stores ...string) error {
	topoProcessor := &TopologyProcessor{
		Name: name,
		Builder: func() sdk.BaseProcessor {
			px := &Process0rNode[Kin, Vin, Kout, Vout]{
				userProcessor: p(),
				outputs:       map[string]GenericProcessor[Kout, Vout]{},
				ctx: &ProcessorContext[Kout, Vout]{
					outputs:      map[string]GenericProcessor[Kout, Vout]{},
					outputErrors: map[string]error{},
				},
			}
			return px
		},
		ChildNodes: []string{},
	}

	topoProcessor.AddChildFunc = func(parent any, child any, childName string) {
		parentNode, ok := parent.(*Process0rNode[Kin, Vin, Kout, Vout])
		if !ok {
			panic("type error")
		}

		childNode, ok := child.(GenericProcessor[Kout, Vout])
		if !ok {
			panic("type error")
		}

		parentNode.outputs[childName] = childNode
		parentNode.ctx.outputs[childName] = childNode
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

	// TODO validate child exists.

	parentNode.ChildNodes = append(parentNode.ChildNodes, child)

	t.processorToParent[child] = parent

	_, ok = t.childNodes[parent]
	if !ok {
		t.childNodes[parent] = []string{}
	}

	t.childNodes[parent] = append(t.childNodes[parent], child)

	return nil

}

var ErrNodeAlreadyExists = errors.New("node exists already")
var ErrNodeNotFound = errors.New("node not found")
var ErrInternal = errors.New("internal")
