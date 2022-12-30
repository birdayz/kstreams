package kstreams

import (
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/exp/slices"
)

// Topology is a fully built DAG that can be used in a kstreams app.
type Topology struct {
	sources    map[string]*TopologySource
	stores     map[string]*TopologyStore
	processors map[string]*TopologyProcessor
	sinks      map[string]*TopologySink
}

func (t *Topology) GetTopics() []string {
	var res []string
	for k := range t.sources {
		res = append(res, k)
	}
	return res
}

func (t *Topology) partitionGroups() []*PartitionGroup {
	var pgs []*PartitionGroup

	// 1. Create single-topic partition groups by traversing the graph, starting
	// at each source.
	for topic := range t.sources { // TODO: make this deterministic, ordering of keys is random

		processors := t.findAllProcessors(topic)

		// Add all state stores attached to these processors
		var storeNames []string
		for _, child := range processors {
			if store, ok := t.processors[child]; ok {
				storeNames = append(storeNames, store.StoreNames...)
			}
		}

		pg := &PartitionGroup{
			sourceTopics:   []string{topic},
			processorNames: processors,
			storeNames:     storeNames,
		}

		pgs = append(pgs, pg)
	}

	// 2. Merge these single-partition PartitionGroups, so all overlaps of graph
	// nodes result are in the same PartitionGroup.
	pgs = mergePartitionGroups(pgs)

	for _, pg := range pgs {
		slices.Sort(pg.sourceTopics)
		slices.Sort(pg.processorNames)
		slices.Sort(pg.storeNames)
	}

	return pgs
}

func (t *Topology) findAllProcessors(processor string) []string {
	var res []string
	if proc, ok := t.processors[processor]; ok {
		children := proc.ChildNodeNames
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

func (t *Topology) CreateTask(topics []string, partition int32, client *kgo.Client) (*Task, error) {
	var srcs []*TopologySource

	for _, topic := range topics {
		src, ok := t.sources[topic]
		if !ok {
			return nil, errors.New("no source found")
		}
		srcs = append(srcs, src)
	}

	stores := map[string]Store{}
	for name, store := range t.stores {
		builtStore, err := store.Build(name, partition)
		if err != nil {
			return nil, fmt.Errorf("failed to build store: %w", err)
		}
		stores[name] = builtStore
	}

	builtProcessors := map[string]BaseProcessor{}

	var neededProcessors []string
	for _, src := range srcs {
		neededProcessors = append(neededProcessors, childNodes(t, src.Name, src.ChildNodeNames...)...)
	}
	neededProcessors = slices.Compact(neededProcessors)

	builtSinks := map[string]Flusher{}

	for _, pr := range neededProcessors {
		topoProcessor, ok := t.processors[pr]
		if ok {
			built := topoProcessor.Build()

			builtProcessors[topoProcessor.Name] = built
		} else {
			topoSink, ok := t.sinks[pr]
			if !ok {

				_, ok := t.sources[pr]

				if !ok {
					return nil, fmt.Errorf("could not find node: %s", pr) // TODO !!!!!!!!!!!! can't find source node
				}
			} else {
				sink := topoSink.Builder(client)
				builtSinks[topoSink.Name] = sink
			}

		}

	}

	builtSources := make(map[string]RecordProcessor)
	for _, topic := range topics {
		builtSources[topic] = t.sources[topic].Build()
	}

	for name, builtSource := range builtSources {
		node := t.sources[name]

		for _, childNodeName := range node.ChildNodeNames {
			childNode, ok := t.processors[childNodeName]
			if ok {
				child, ok := builtProcessors[childNode.Name]
				if !ok {
					return nil, fmt.Errorf("processor [%s] not found", childNode.Name)
				}
				node.AddChildFunc(builtSource, child, childNode.Name)
			} else {
				childSink, ok := t.sinks[childNodeName]
				if !ok {
					return nil, fmt.Errorf("could not find child node %s", childNodeName)
				}

				child, ok := builtSinks[childSink.Name]
				if !ok {
					return nil, fmt.Errorf("could not find child node %s", childNodeName)
				}

				node.AddChildFunc(builtSource, child, childSink.Name)

			}

		}
	}

	// Link sub-processors
	for k, builtProcessor := range builtProcessors {
		node := t.processors[k]

		for _, childNodeName := range node.ChildNodeNames {
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

	processorStores := make(map[string][]string)
	for _, proc := range neededProcessors {
		processorStores[proc] = t.processors[proc].ChildNodeNames
	}

	task := NewTask(topics, partition, builtSources, stores, builtProcessors, builtSinks, processorStores)
	return task, nil

}

// appendChildren gets a list of all node names of the given processor (?)
func childNodes(t *Topology, p string, childNodeNames ...string) []string {
	var res []string
	res = append(res, childNodeNames...)
	for _, child := range childNodeNames {
		childProcessor, ok := t.processors[child]
		if ok {
			res = append(res, childNodes(t, childProcessor.Name, childProcessor.ChildNodeNames...)...)
		} else {
			_, ok := t.sinks[child]
			if !ok {
				panic("failed to find processor or sink")
				// TODO FIXME handle. Neither processor, not sink found.
			}

			// It's a sink. Since this is terminal, we don't need to go further and
			// can just add the name
			res = append(res, child)
		}

	}
	return res
}
