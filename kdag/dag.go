package kdag

import (
	"slices"

	"github.com/birdayz/kstreams/internal/coordination"
)

// DAG is a fully built processing graph that can be used in a kstreams app.
type DAG struct {
	graph           *Graph
	partitionGroups []*coordination.PartitionGroup
}

// GetTopics returns all source topics in deterministic order.
func (d *DAG) GetTopics() []string {
	topics := make([]string, 0, len(d.graph.Sources))
	for topic := range d.graph.Sources {
		topics = append(topics, topic)
	}
	slices.Sort(topics)
	return topics
}

// GetPartitionGroups returns the precomputed partition groups.
func (d *DAG) GetPartitionGroups() []*coordination.PartitionGroup {
	return d.partitionGroups
}

// GetGraph returns the underlying graph for task building.
func (d *DAG) GetGraph() *Graph {
	return d.graph
}

// HasStatefulProcessors returns true if the topology has any state stores registered.
// Used to validate that WithStateDir() is configured for stateful topologies.
func (d *DAG) HasStatefulProcessors() bool {
	return len(d.graph.Stores) > 0
}
