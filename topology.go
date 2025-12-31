package kstreams

import (
	"slices"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Topology is a fully built DAG that can be used in a kstreams app.
type Topology struct {
	// Graph-based implementation
	graph           *TopologyGraph
	partitionGroups []*PartitionGroup

	// OLD architecture - being removed
	// stores     map[string]*TopologyStore
	sources    map[string]*TopologySource
	processors map[string]*TopologyProcessor
	sinks      map[string]*TopologySink
}

func (t *Topology) GetTopics() []string {
	// Use new graph for deterministic ordering
	topics := make([]string, 0, len(t.graph.sources))
	for topic := range t.graph.sources {
		topics = append(topics, topic)
	}
	slices.Sort(topics) // DETERMINISTIC!
	return topics
}

// PartitionGroup is a sub-graph of nodes that must be co-partitioned as they depend on each other.
type PartitionGroup struct {
	sourceTopics   []string
	processorNames []string
	storeNames     []string
}

func (t *Topology) getPartitionGroups() []*PartitionGroup {
	// Use precomputed partition groups from the new graph
	// These are computed deterministically during Build()
	return t.partitionGroups
}

func (t *Topology) findAllProcessors(source string) []string {
	var res []string
	if proc, ok := t.sources[source]; ok {
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

func (t *Topology) CreateTask(topics []string, partition int32, client *kgo.Client, appID string, stateDir string) (*Task, error) {
	// Delegate to the new graph's simplified BuildTask method
	// This replaces the old 125-line multi-pass construction with a clean 2-pass approach
	return t.graph.BuildTask(topics, partition, client, appID, stateDir)
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

// If there is any overlap in the input partition groups, they are merged together.
func mergePartitionGroups(pgs []*PartitionGroup) []*PartitionGroup {
	finished := false
	for !finished {
		pgs, finished = mergeIteration(pgs)
	}

	return pgs
}
